WITH cleaned AS (
    SELECT
        *,
        COALESCE(cost, 0) AS cost_filled,
        COALESCE(brand, 'DIRECT') AS brand_clean
    FROM products
    WHERE launch_date > TO_DATE('2023-03-04')
      AND supplier_id IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        product_name,
        category,
        cost,
        tags,
        supplier_id,
        cost_filled,
        brand_clean,
        AVG(price) OVER w_running AS price_avg_2_prior,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty DESC, id) AS seq_no,
        LAST_VALUE(cost) IGNORE NULLS OVER w_grp AS last_known_cost,
        CORR(price, stock_qty) OVER (PARTITION BY brand) AS corr_price_stock_qty,
        DENSE_RANK() OVER (PARTITION BY supplier_id, category ORDER BY price DESC) AS price_dense_rank
    FROM cleaned
    WINDOW w_running AS (PARTITION BY brand, supplier_id ORDER BY launch_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), w_grp AS (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY supplier_id, category ORDER BY cost NULLS FIRST) AS rank_in_group
    FROM with_windows
    QUALIFY rank_in_group = 1
),
with_dims AS (
    SELECT e.*, b.rating
    FROM ranked e
    LEFT JOIN suppliers b
        ON e.supplier_id <=> b.id
),
wide AS (
    SELECT *
    FROM (
        SELECT
            supplier_id,
            category,
            cost
        FROM with_dims
    ) src
    PIVOT (
        AVG(cost) AS total,
        COUNT(1) AS n
        FOR category IN ('Books', 'Electronics', 'Grocery')
    )
)
SELECT
    supplier_id,
    Books_total,
    Electronics_total,
    Electronics_n,
    Grocery_total,
    Grocery_n
FROM wide
ORDER BY Electronics_n NULLS FIRST;
