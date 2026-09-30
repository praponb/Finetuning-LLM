WITH with_dims AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.sale_date,
        d.sqft,
        d.opened_date
    FROM store_sales src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
enriched AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.sale_date,
        src.sqft,
        p.price,
        p.cost,
        p.product_name
    FROM with_dims src
    LEFT JOIN products p
        ON src.product_id = p.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        price,
        cost,
        product_name,
        CASE WHEN cost >= 750 THEN 'High' WHEN cost >= 360 THEN 'Medium' ELSE 'Low' END AS cost_band,
        GREATEST(cost, price) AS greatest_cost_price
    FROM enriched
    WHERE sqft >= 6286
),
enriched_store_sales AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.price,
        e.cost,
        e.product_name,
        e.cost_band,
        d.product_name AS product_product_name
    FROM valid e
    LEFT JOIN products d
        ON e.product_id = d.id
    WHERE d.category = 'Books'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(price) OVER (PARTITION BY product_id) AS product_id_min_price,
        FIRST_VALUE(price) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_price
    FROM enriched_store_sales
)
SELECT
    id,
    product_name,
    cost_band,
    product_id_min_price,
    first_known_price,
    CASE WHEN product_name RLIKE '^(Running Shoes|USB-C Hub)$' THEN 'CORE' WHEN product_name RLIKE '^(Laptop Pro 14|wireless mouse)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS product_name_zone
FROM with_windows;
