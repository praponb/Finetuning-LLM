WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', supplier_name, country) supplier_name_country_key,
        COALESCE(city, 'OTHER') city_filled
    FROM suppliers
    WHERE city IS DISTINCT FROM 'Tokyo'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.product_name LIKE 'Yo%'
        )
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        city_filled,
        CASE WHEN city = 'New York' THEN 1 ELSE 0 END is_new_york,
        ROUND(rating, 2) rating_rounded
    FROM kept
    WHERE city = 'New York'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.cost >= 530
        )
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        supplier_name,
        city,
        rating,
        city_filled,
        rating_rounded
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY is_new_york NULLS FIRST, id) AS rank_in_group
        FROM eligible
    ) t
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER w_running rating_bucket,
        RANK() OVER (PARTITION BY city ORDER BY rating_rounded DESC NULLS FIRST) rating_rounded_rank,
        ROW_NUMBER() OVER w_running row_seq,
        LEAD(rating_rounded, 1) OVER w_ordered next_rating_rounded
    FROM deduped
    WINDOW w_running AS (PARTITION BY city ORDER BY rating, id), w_ordered AS (PARTITION BY city ORDER BY rating_rounded, id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(rating_rounded_rank, TRUE) OVER (PARTITION BY city ORDER BY rating_rounded_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_rating_rounded_rank,
        LAG(rating_rounded_rank, 2) OVER (PARTITION BY city ORDER BY next_rating_rounded, id) prev_rating_rounded_rank,
        LEAD(row_seq, 2) OVER (ORDER BY next_rating_rounded, id) next_row_seq,
        AVG(rating_bucket) OVER (PARTITION BY city ORDER BY rating_rounded, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) rating_bucket_avg_3_prior,
        NTILE(3) OVER (PARTITION BY city ORDER BY rating, id) rating_bucket_2
    FROM with_windows
    WHERE row_seq != 8
)
SELECT
    id,
    rating,
    city_filled,
    rating_rounded,
    rating_bucket,
    rating_rounded_rank,
    row_seq,
    next_rating_rounded,
    last_known_rating_rounded_rank
FROM enriched
ORDER BY rating_bucket;
