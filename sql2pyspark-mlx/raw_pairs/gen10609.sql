WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        ROUND(units * 1.15, 2) AS adjusted_units
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-07-23')
),
joined AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.sale_date,
        src.adjusted_units,
        b.cost,
        b.launch_date,
        b.brand
    FROM valid src
    LEFT JOIN products b
        ON src.product_id = b.id
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.sale_date,
        src.cost,
        src.launch_date,
        b.launch_date AS product_launch_date,
        b.cost AS product_cost,
        b.brand AS product_brand
    FROM joined src
    LEFT JOIN products b
        ON src.product_id = b.id
    WHERE b.supplier_id = 39
),
joined_store_sales AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.launch_date,
        s.product_cost,
        p.price,
        p.stock_qty,
        p.cost AS product_cost_2
    FROM with_dims s
    LEFT JOIN products p
        ON s.product_id = p.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        price,
        product_cost_2,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY launch_date DESC, id) AS event_seq,
        AVG(product_cost_2) OVER (PARTITION BY product_id ORDER BY product_cost_2, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS product_cost_2_avg_2
    FROM joined_store_sales
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(product_cost_2_avg_2, 0) AS product_cost_2_avg_2_rounded
    FROM with_windows
    WHERE product_id IN (5, 6)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        product_id,
        product_cost_2_avg_2_rounded,
        product_cost_2,
        price
    FROM cleaned
) src
UNPIVOT (
    reading FOR kpi IN (product_cost_2_avg_2_rounded AS PRODUCT_COST_2_AVG_2_ROUNDED, product_cost_2, price AS PRICE_VAL)
);
