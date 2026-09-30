WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        country,
        city,
        CONCAT_WS('-', country, supplier_name) AS country_supplier_name_key,
        CASE WHEN rating >= 3.8 THEN 'High' WHEN rating >= 1.7 THEN 'Medium' ELSE 'Low' END AS rating_band
    FROM suppliers
    WHERE city IS NOT NULL
      AND city IS DISTINCT FROM 'Bangkok'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.brand = 'Globex'
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.category IN ('Fashion', 'Sports')
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id = 12
        )
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        latest.price
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Ranking and gap features
SELECT
    id,
    country,
    AVG(price) OVER (PARTITION BY country ORDER BY price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS price_avg_6_prior,
    AVG(price) OVER (PARTITION BY country ORDER BY price, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS price_avg_3_prior,
    ROUND(price / SUM(price) OVER (PARTITION BY country), 4) AS price_share
FROM with_latest
