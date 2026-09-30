WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        CEIL(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock >= DATE '2023-04-06'
      AND last_restock <= '2025-07-01'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(quantity_whole) AS quantity_whole_whole,
        DATEDIFF(CURRENT_DATE(), last_restock) / 365.25 AS years_since_last_restock
    FROM filtered
    WHERE store_id IN (1, 3, 4)
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.quantity_whole_whole,
        d.city,
        d.region
    FROM valid s
    INNER JOIN stores d
        ON s.store_id = d.id
    WHERE YEAR(d.opened_date) >= 2025
),
joined AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.region,
        p.product_name,
        p.category,
        p.price
    FROM with_dims s
    LEFT JOIN products p
        ON s.product_id = p.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_id,
        category
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY price, id) AS dedup_rank
        FROM joined
    ) ranked
    WHERE dedup_rank = 1
)
SELECT
    s.id,
    s.store_id,
    (SELECT MAX(x.revenue) FROM store_sales x WHERE x.store_id = s.store_id) AS max_store_sale_revenue
FROM ranked s
