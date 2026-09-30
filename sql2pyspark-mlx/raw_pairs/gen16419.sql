WITH filtered AS (
    SELECT
        id,
        sale_date,
        units,
        revenue,
        ADD_MONTHS(sale_date, 12) AS sale_date_plus_months
    FROM store_sales
    WHERE sale_date > '2024-11-01'
),
filtered_store_sales AS (
    SELECT
        id,
        sale_date,
        units,
        revenue,
        sale_date_plus_months,
        QUARTER(sale_date_plus_months) AS sale_date_plus_months_quarter,
        DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM filtered
    WHERE sale_date BETWEEN '2023-03-01' AND '2023-05-01'
),
scored AS (
    SELECT
        id,
        units,
        revenue,
        sale_date_plus_months_quarter,
        days_since_sale_date,
        NTILE(3) OVER (ORDER BY revenue, id) AS revenue_bucket,
        AVG(sale_date_plus_months_quarter) OVER (ORDER BY sale_date_plus_months, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS sale_date_plus_months_quarter_avg_3_prior,
        SUM(days_since_sale_date) OVER (PARTITION BY MONTH(sale_date_plus_months) ORDER BY days_since_sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_days_since_sale_date
    FROM filtered_store_sales
),
scored_store_sales AS (
    SELECT
        *,
        LAG(cumulative_days_since_sale_date) OVER w_hist AS prev_cumulative_days_since_sale_date,
        LEAD(cumulative_days_since_sale_date) OVER w_hist AS next_cumulative_days_since_sale_date,
        LAG(revenue_bucket) OVER w_hist AS prev_revenue_bucket,
        ROW_NUMBER() OVER w_hist AS seq_no
    FROM scored
    WINDOW w_hist AS (ORDER BY cumulative_days_since_sale_date, id)
),
kept_keys AS (
    SELECT id
    FROM scored_store_sales
    EXCEPT
    SELECT id
    FROM store_sales
    WHERE product_id = 6
),
kept_rows AS (
    SELECT s.*
    FROM scored_store_sales s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    seq_no,
    COUNT(*) AS cnt,
    ROUND(AVG(days_since_sale_date), 2) AS avg_days_since_sale_date,
    COUNT(DISTINCT id) AS distinct_ids
FROM kept_rows
GROUP BY 1
ORDER BY seq_no DESC NULLS FIRST;
