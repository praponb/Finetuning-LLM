WITH kept_keys AS (
    SELECT store_id, id
    FROM store_sales
    INTERSECT ALL
    SELECT store_id, id
    FROM store_sales
    WHERE product_id IN (2, 5, 9, 10)
),
kept_rows AS (
    SELECT s.*
    FROM store_sales s
    INNER JOIN kept_keys k ON s.store_id = k.store_id AND s.id = k.id
),
base AS (
    SELECT *, DATE_SUB(sale_date, 56) sale_date_minus_56d
    FROM kept_rows
    WHERE product_id IN (4, 5, 8)
),
kept_keys_2 AS (
    SELECT store_id, id
    FROM base
    EXCEPT
    SELECT store_id, id
    FROM store_sales
    WHERE units BETWEEN 18 AND 22
),
verified AS (
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys_2 k ON s.store_id = k.store_id AND s.id = k.id
),
cleaned AS (
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        LAST_DAY(sale_date_minus_56d) sale_date_minus_56d_month_end,
        FLOOR(revenue) revenue_whole
    FROM verified
    WHERE sale_date > TO_DATE('2023-08-13')
      AND units != 33
),
with_dims AS (
    SELECT s.*, p.launch_date
    FROM cleaned s
    LEFT JOIN products p
        ON s.product_id = p.id
)
SELECT
    product_id,
    sale_date,
    units,
    revenue,
    sale_date_minus_56d_month_end,
    revenue_whole,
    launch_date
FROM with_dims;
