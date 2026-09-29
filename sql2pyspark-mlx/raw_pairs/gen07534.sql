WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        DATEDIFF(CURRENT_DATE(), sale_date) / 365 AS years_since_sale_date
    FROM store_sales
    WHERE sale_date >= DATE '2023-07-07'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM valid
    EXCEPT ALL
    SELECT product_id, id
    FROM store_sales
    WHERE units = 26
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id AND s.id = k.id
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.sale_date,
        e.units,
        e.years_since_sale_date,
        p.category,
        p.product_name,
        p.cost
    FROM verified e
    LEFT JOIN products p
        ON e.product_id = p.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT *, ROUND(cost / SUM(cost) OVER (PARTITION BY product_id), 4) AS cost_share
    FROM with_dims
    WHERE store_id IN (1, 2, 3, 4)
),
joined AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.years_since_sale_date,
        e.cost,
        e.cost_share,
        d.cost AS product_cost,
        d.stock_qty
    FROM metrics e
    INNER JOIN products d
        ON e.product_id = d.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, product_id
    FROM joined
    EXCEPT
    SELECT id, product_id
    FROM store_sales
    WHERE MONTH(sale_date) IN (2, 10, 11)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN kept_keys k ON s.id = k.id AND s.product_id = k.product_id
)
SELECT
    store_id,
    years_since_sale_date,
    cost,
    cost_share,
    product_cost
FROM reconciled
ORDER BY store_id DESC;
