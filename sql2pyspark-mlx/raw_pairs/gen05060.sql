WITH valid AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        SORT_ARRAY(tags) sorted_tags
    FROM products
    WHERE launch_date > TO_DATE('2023-03-20')
      AND brand IS DISTINCT FROM 'Initech'
),
not_flagged AS (
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.last_restock BETWEEN '2025-01-11' AND '2025-08-01'
        )
),
metrics AS (
    SELECT
        id,
        product_name,
        price,
        cost,
        tags,
        MAX(cost) OVER w_ordered running_max_cost,
        MIN(cost) OVER w_ordered running_min_cost,
        SUM(cost) OVER w_ordered running_sum_cost
    FROM not_flagged
    WINDOW w_ordered AS (PARTITION BY supplier_id ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.quantity >= 185
        )
),
matched_keys AS (
    SELECT id
    FROM eligible
    EXCEPT DISTINCT
    SELECT id
    FROM products
    WHERE brand NOT IN ('Umbrella', 'Stark', 'Initech')
),
kept_rows AS (
    SELECT s.*
    FROM eligible s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    id,
    price,
    cost,
    running_max_cost,
    running_min_cost,
    running_sum_cost
FROM kept_rows
ORDER BY running_max_cost DESC NULLS LAST, running_sum_cost ASC
