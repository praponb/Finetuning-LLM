WITH filtered AS (
    SELECT
        id,
        product_id,
        revenue,
        LAST_DAY(sale_date) AS sale_date_month_end
    FROM store_sales
    WHERE revenue > 3700
),
with_dims AS (
    SELECT
        src.*,
        p.category,
        p.stock_qty
    FROM filtered src
    LEFT JOIN products p
        ON src.product_id = p.id
),
enriched AS (
    SELECT
        src.*,
        p.cost,
        p.stock_qty AS product_stock_qty
    FROM with_dims src
    INNER JOIN products p
        ON src.product_id = p.id
),
filtered_store_sales AS (
    SELECT
        *,
        cost - revenue AS cost_minus_revenue,
        DATEDIFF(CURRENT_DATE(), sale_date_month_end) AS days_since_sale_date_month_end
    FROM enriched
    WHERE product_id = 4
),
by_type AS (
    SELECT *
    FROM (
        SELECT
            product_id,
            category,
            days_since_sale_date_month_end
        FROM filtered_store_sales
    ) src
    PIVOT (
        AVG(days_since_sale_date_month_end) AS value_sum,
        COUNT(1) AS cnt
        FOR category IN ('Home', 'Electronics', 'Sports')
    )
)
SELECT
    product_id,
    Home_value_sum,
    Home_cnt,
    Sports_value_sum
FROM by_type;
