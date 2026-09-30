WITH enriched AS (
    SELECT
        a.*,
        d.country,
        d.supplier_name
    FROM products a
    LEFT JOIN suppliers d
        ON a.supplier_id = d.id
),
filtered AS (
    SELECT
        *,
        SUBSTRING(brand, 1, 4) AS brand_prefix,
        ARRAY_JOIN(tags, '|') AS tags_text,
        COALESCE(brand, 'N/A') AS brand_filled
    FROM enriched
    WHERE launch_date >= TO_DATE('2023-04-21')
),
latest_match AS (
    SELECT s.*, latest.store_sale_revenue
    FROM filtered s
    JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    country,
    SUM(CASE WHEN brand = 'Globex' THEN store_sale_revenue ELSE 0 END) AS globex_store_sale_revenue,
    SIZE(COLLECT_SET(brand)) AS num_brands
FROM latest_match
GROUP BY country
ORDER BY country DESC
