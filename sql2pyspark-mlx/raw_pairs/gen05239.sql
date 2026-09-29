WITH base AS (
    SELECT *, LAST_DAY(sale_date) AS sale_date_month_end
    FROM store_sales
    WHERE product_id IN (2, 3, 8, 9)
),
joined AS (
    SELECT
        src.*,
        b.store_name,
        b.region,
        b.opened_date
    FROM base src
    LEFT JOIN stores b
        ON src.store_id = b.id
),
deduped AS (
    SELECT
        id,
        sale_date,
        units,
        revenue,
        sale_date_month_end,
        store_name
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region, store_id ORDER BY opened_date NULLS FIRST, id) = 1
),
with_windows AS (
    SELECT
        *,
        AVG(revenue) OVER (ORDER BY sale_date_month_end, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_revenue,
        SUM(revenue) OVER (ORDER BY sale_date_month_end, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_revenue,
        COUNT(revenue) OVER (ORDER BY sale_date_month_end, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_revenue
    FROM deduped
    WHERE store_name LIKE '%Downto%'
),
kept_keys AS (
    SELECT id
    FROM with_windows
    EXCEPT ALL
    SELECT id
    FROM store_sales
    WHERE units > 22
),
verified AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    id,
    revenue,
    sale_date_month_end,
    last3_sum_revenue,
    last3_count_revenue
FROM verified
ORDER BY sale_date_month_end DESC NULLS LAST, last3_count_revenue;
