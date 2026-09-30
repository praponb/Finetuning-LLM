WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_SUB(last_restock, 58) AS last_restock_minus_58d
    FROM inventory
    WHERE last_restock >= DATE '2023-06-12'
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.*,
        p.brand,
        p.category
    FROM valid s
    LEFT JOIN products p
        ON s.product_id = p.id
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.*,
        b.store_name,
        b.region,
        b.sqft
    FROM enriched a
    LEFT JOIN stores b
        ON a.store_id = b.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY brand ORDER BY last_restock DESC NULLS FIRST) AS rank_in_group
        FROM with_dims
    ) t
    WHERE rank_in_group = 1
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.last_restock,
    s.last_restock_minus_58d,
    s.brand,
    s.region,
    s.sqft,
    s.rank_in_group,
    CASE WHEN region RLIKE '^(West|North)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS region_zone,
    (SELECT MAX(t2.shipping_fee) FROM orders t2 WHERE t2.store_id = s.store_id) AS max_order_shipping_fee
FROM deduped s
ORDER BY max_order_shipping_fee DESC NULLS FIRST;
