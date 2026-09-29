WITH cleaned AS (
    SELECT *, ROUND(quantity, 0) AS quantity_rounded
    FROM inventory
    WHERE YEAR(last_restock) >= 2023
),
cleaned_inventory AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), last_restock) / 30 AS months_since_last_restock
    FROM cleaned
    WHERE last_restock > TO_DATE('2023-05-22')
      AND store_id IN (2, 3, 4)
),
base AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock,
        YEAR(last_restock) AS last_restock_year
    FROM cleaned_inventory
    WHERE last_restock > TO_DATE('2023-02-21')
),
valid AS (
    SELECT
        *,
        last_restock_year * quantity_rounded AS last_restock_year_times_quantity_rounded,
        LAST_DAY(last_restock) AS last_restock_month_end
    FROM base
    WHERE last_restock >= DATE '2023-03-01'
),
joined AS (
    SELECT s.*, d.product_name
    FROM valid s
    INNER JOIN products d
        ON s.product_id = d.id
),
scored AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_rounded,
        months_since_last_restock,
        last_restock_year,
        last_restock_year_times_quantity_rounded,
        last_restock_month_end,
        product_name,
        SUM(months_since_last_restock) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_months_since_last_restock,
        MIN(months_since_last_restock) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_months_since_last_restock,
        MAX(months_since_last_restock) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_months_since_last_restock
    FROM joined
)
SELECT
    dim.category,
    COUNT(*) AS num_rows,
    MAX(a.last_restock_year) AS max_last_restock_year,
    GROUPING_ID() AS level_id
FROM scored a
INNER JOIN products dim ON a.product_id = dim.id
GROUP BY ROLLUP (dim.category);
