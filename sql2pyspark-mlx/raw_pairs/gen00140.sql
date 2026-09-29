WITH with_last AS (
    SELECT s.*, x.product_launch_date
    FROM suppliers s
    JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rank_in_group
        FROM with_last
    ) t
    WHERE rank_in_group = 1
),
metrics AS (
    SELECT
        id,
        country,
        city,
        rating,
        product_launch_date,
        rank_in_group,
        LAG(rating, 1) OVER (PARTITION BY city ORDER BY rank_in_group, id) AS prev_rating,
        RANK() OVER (PARTITION BY country ORDER BY rating DESC NULLS LAST) AS rating_rank
    FROM latest
),
latest_match AS (
    SELECT
        s.id,
        s.country,
        s.city,
        s.rating,
        s.prev_rating,
        s.rating_rank,
        latest.category,
        latest.product_price
    FROM metrics s
    JOIN LATERAL (
            SELECT c.category, c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY product_price, id) AS rank_in_group
    FROM latest_match
    QUALIFY rank_in_group = 1
)
SELECT
    CASE WHEN rating >= 4.0 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
    COUNT_IF(country = 'DE') AS de_events,
    COUNT_IF(country = 'SG') AS sg_events,
    SIZE(COLLECT_SET(city)) AS num_cities,
    ROUND(SUM(rating_rank) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS rating_rank_per_chiang_mai
FROM ranked
GROUP BY CASE WHEN rating >= 4.0 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING sg_events > 1
