WITH cleaned AS (
    SELECT *, CONCAT(city, ' ', country) AS city_country
    FROM suppliers
    WHERE city IN ('Chiang Mai', 'Singapore')
),
with_last AS (
    SELECT
        s.id,
        s.supplier_name,
        s.country,
        s.rating,
        s.city_country,
        latest.product_cost,
        latest.product_stock_qty
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.cost AS product_cost, c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY product_cost ASC, id ASC) AS latest_rank
        FROM with_last
    ) t
    WHERE latest_rank = 1
),
not_flagged AS (
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.brand IN ('Stark', 'Acme')
        )
),
remaining_keys AS (
    SELECT id, country
    FROM not_flagged
    EXCEPT DISTINCT
    SELECT id, country
    FROM suppliers
    WHERE country IN ('JP', 'TH', 'DE')
),
verified AS (
    SELECT s.*
    FROM not_flagged s
    JOIN remaining_keys k ON s.id = k.id AND s.country = k.country
),
enriched AS (
    SELECT
        country,
        product_cost,
        product_stock_qty,
        latest_rank,
        ROUND(product_cost / SUM(product_cost) OVER (PARTITION BY country), 4) AS product_cost_share,
        PERCENT_RANK() OVER (ORDER BY rating ASC NULLS LAST) AS rating_percent_rank,
        RANK() OVER (PARTITION BY country ORDER BY product_stock_qty ASC) AS product_stock_qty_rank
    FROM verified
    WHERE product_cost <= 450
)
SELECT
    country,
    product_cost,
    product_stock_qty,
    latest_rank,
    product_cost_share,
    CASE WHEN country RLIKE '^(UK|JP)$' THEN 'PRIORITY' WHEN country RLIKE '^(TH|US)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS country_zone
FROM enriched
ORDER BY product_cost ASC NULLS FIRST;
