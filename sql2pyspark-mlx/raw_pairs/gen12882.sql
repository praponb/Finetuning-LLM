WITH latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.price,
        latest.product_category
    FROM suppliers s
    LEFT JOIN LATERAL (
            SELECT c.price, c.category AS product_category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY city, product_category ORDER BY rating DESC) AS row_num
    FROM latest_match
    QUALIFY row_num = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.supplier_id IS NULL
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date > '2024-10-01'
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND ARRAY_CONTAINS(c.tags, 'clearance')
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(price) OVER (PARTITION BY product_category) AS product_category_stddev_samp_price,
        LAG(price, 1) OVER (ORDER BY rating, id) AS prev_price,
        AVG(rating) OVER (PARTITION BY city) AS city_avg_rating,
        ROUND(price / SUM(price) OVER (PARTITION BY product_category, country), 4) AS price_share
    FROM kept
)
-- Aggregate with conditional metrics
SELECT
    product_category,
    APPROX_PERCENTILE(price_share, ARRAY(0.25, 0.5, 0.75)) AS price_share_quartiles,
    SIZE(COLLECT_SET(country)) AS num_countries,
    AVG(price) FILTER (WHERE city = 'Singapore') AS singapore_avg_price,
    APPROX_PERCENTILE(city_avg_rating, 0.9) AS city_avg_rating_p90
FROM enriched
GROUP BY product_category
HAVING AVG(price) FILTER (WHERE city = 'Singapore') > 0;
