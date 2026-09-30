WITH cleaned AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        INITCAP(TRIM(city)) city_title,
        COALESCE(city, 'UNKNOWN') city_filled
    FROM suppliers
    WHERE city IN ('Singapore', 'Tokyo')
),
scored AS (
    SELECT
        *,
        LAG(rating, 2) OVER (ORDER BY rating, id) prev_rating,
        NTILE(5) OVER (PARTITION BY city ORDER BY rating, id) rating_bucket,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) rating_share,
        NTILE(3) OVER (PARTITION BY country ORDER BY rating DESC, id) rating_bucket_2
    FROM cleaned
),
deduped AS (
    SELECT
        id,
        supplier_name,
        country,
        city_title,
        prev_rating,
        rating_bucket,
        rating_share,
        rating_bucket_2
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating DESC, id) AS dedup_rank
        FROM scored
    ) t
    WHERE dedup_rank = 1
),
deduped_suppliers AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating_bucket_2, id) AS rank_in_group
        FROM deduped
    ) t
    WHERE rank_in_group = 1
),
eligible AS (
    SELECT s.*
    FROM deduped_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.brand IN ('Acme', 'Stark', 'Umbrella')
        )
),
metrics AS (
    SELECT
        id,
        supplier_name,
        city_title,
        prev_rating,
        rating_bucket,
        rank_in_group,
        LAG(rating_share) OVER w_hist prev_rating_share,
        LAG(prev_rating) OVER w_hist prev_prev_rating,
        ROW_NUMBER() OVER w_hist event_seq
    FROM eligible
    WHERE rating_bucket >= 4
    WINDOW w_hist AS (PARTITION BY country ORDER BY rank_in_group, id)
),
stats AS (
    SELECT
        CASE WHEN prev_rating_share >= 0.85 THEN 'HIGH' ELSE 'LOW' END AS prev_rating_share_tier,
        ROUND(AVG(rating_bucket), 2) AS avg_rating_bucket,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT id) AS distinct_ids_2,
        PERCENTILE_APPROX(prev_rating, ARRAY(0.25, 0.5, 0.75)) AS prev_rating_quartiles,
        ROUND(AVG(prev_rating), 2) AS avg_prev_rating
    FROM metrics
    GROUP BY CASE WHEN prev_rating_share >= 0.85 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    prev_rating_share_tier,
    avg_rating_bucket,
    distinct_ids_2,
    avg_prev_rating
FROM stats
