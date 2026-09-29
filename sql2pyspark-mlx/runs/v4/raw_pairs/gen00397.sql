WITH eligible AS (
    SELECT s.*
    FROM suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND ARRAY_CONTAINS(x.tags, 'new')
        )
),
enriched AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        SUM(rating) OVER (PARTITION BY city) AS city_sum_rating,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM eligible
    WINDOW w_recent AS (PARTITION BY country ORDER BY rating, id)
),
cleaned AS (
    SELECT
        id,
        supplier_name,
        country,
        rating,
        running_rating,
        rating_share,
        city_sum_rating,
        row_seq,
        CEIL(row_seq) AS row_seq_whole,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM enriched
    WHERE city IS NOT NULL
      AND city_sum_rating >= 11
),
eligible_suppliers AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.product_name LIKE '%Chai%'
        )
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY row_seq_whole, id) AS seq_no,
        PERCENT_RANK() OVER (PARTITION BY country ORDER BY running_rating NULLS FIRST) AS running_rating_percent_rank,
        NTILE(3) OVER (PARTITION BY country ORDER BY row_seq_whole DESC, id) AS row_seq_whole_bucket,
        AVG(running_rating) OVER (PARTITION BY country ORDER BY row_seq, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS running_rating_avg_5_prior,
        AVG(row_seq_whole) OVER (ORDER BY row_seq_whole, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS row_seq_whole_avg_3_prior
    FROM eligible_suppliers
),
latest_match AS (
    SELECT s.*, top1.product_brand
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
stats AS (
    SELECT
        seq_no,
        product_brand,
        SORT_ARRAY(COLLECT_SET(country)) AS country_list,
        COUNT(*) AS event_count
    FROM latest_match
    GROUP BY 1, 2
    HAVING COUNT(*) < 1
)
SELECT
    seq_no,
    product_brand,
    country_list
FROM stats;
