WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC NULLS FIRST, id) AS rank_in_group
    FROM store_sales
    QUALIFY rank_in_group = 1
),
joined AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.rank_in_group,
        p.store_name,
        p.sqft,
        p.opened_date
    FROM latest s
    LEFT JOIN stores p
        ON s.store_id = p.id
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.rank_in_group,
        s.store_name,
        b.store_name AS store_store_name,
        b.region
    FROM joined s
    LEFT JOIN stores b
        ON s.store_id = b.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(region, TRUE) OVER (PARTITION BY region ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY rank_in_group DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY rank_in_group DESC, id) AS event_seq,
        LAST_VALUE(region, TRUE) OVER (PARTITION BY product_id ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region_2
    FROM with_dims
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.store_name,
    s.region,
    s.last_known_region,
    s.row_seq,
    s.event_seq,
    s.last_known_region_2,
    (SELECT MAX(x.quantity) FROM inventory x WHERE x.product_id = s.product_id) AS max_inventory_quantity
FROM enriched s
ORDER BY region;
