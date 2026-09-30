WITH with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.price
    FROM suppliers s
    LEFT JOIN LATERAL (
            SELECT c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY country ORDER BY rating DESC NULLS LAST) AS rating_rank,
        ROUND(price / SUM(price) OVER (PARTITION BY city), 4) AS price_share,
        SUM(rating) OVER (PARTITION BY city ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating
    FROM with_last
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(price) OVER (PARTITION BY city) AS city_max_price,
        ROUND(cumulative_rating / SUM(cumulative_rating) OVER (PARTITION BY country), 4) AS cumulative_rating_share
    FROM metrics
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.cost >= 530
        )
),
stats AS (
    -- Multi-level aggregation
    SELECT
        city,
        STDDEV_SAMP(rating) AS rating_stddev,
        ROUND(AVG(cumulative_rating_share), 2) AS avg_cumulative_rating_share,
        COUNT(*) FILTER (WHERE country = 'DE') AS de_rows,
        COUNT(*) AS row_count
    FROM not_flagged
    GROUP BY ROLLUP (city)
)
SELECT avg_cumulative_rating_share, row_count
FROM stats
