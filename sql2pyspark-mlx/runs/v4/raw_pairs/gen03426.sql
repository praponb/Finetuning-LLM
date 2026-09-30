WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ADD_MONTHS(sale_date, 6) AS sale_date_plus_months,
        DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM store_sales
    WHERE units < 31
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        sale_date_plus_months,
        days_since_sale_date,
        LAST_VALUE(days_since_sale_date) IGNORE NULLS OVER (PARTITION BY store_id, product_id ORDER BY days_since_sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_sale_date,
        AVG(days_since_sale_date) OVER (PARTITION BY store_id, product_id ORDER BY sale_date_plus_months, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS days_since_sale_date_avg_2_prior,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY days_since_sale_date DESC NULLS LAST) AS days_since_sale_date_percent_rank,
        LEAD(days_since_sale_date, 1) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS next_days_since_sale_date,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY sale_date, id) AS row_seq
    FROM filtered
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN last_known_days_since_sale_date >= 70 THEN 'High' WHEN last_known_days_since_sale_date >= 31 THEN 'Medium' ELSE 'Low' END AS last_known_days_since_sale_date_level
    FROM metrics
    WHERE units > 38
),
joined AS (
    -- Attach stores attributes
    SELECT a.*, d.store_name
    FROM base a
    LEFT JOIN stores d
        ON a.store_id = d.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(revenue, 1) OVER (PARTITION BY store_id ORDER BY revenue, id) AS prev_revenue,
        NTILE(10) OVER w_recent AS revenue_bucket,
        FIRST_VALUE(days_since_sale_date_avg_2_prior) IGNORE NULLS OVER (PARTITION BY product_id, store_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_sale_date_avg_2_prior,
        NTILE(3) OVER w_hist AS days_since_sale_date_avg_2_prior_bucket,
        MAX(revenue) OVER (PARTITION BY product_id) AS product_id_max_revenue
    FROM joined
    WINDOW w_recent AS (ORDER BY revenue DESC, id), w_hist AS (PARTITION BY store_id ORDER BY days_since_sale_date_avg_2_prior, id)
)
SELECT
    id,
    store_id,
    product_id,
    revenue,
    sale_date_plus_months,
    days_since_sale_date,
    last_known_days_since_sale_date,
    days_since_sale_date_avg_2_prior,
    days_since_sale_date_percent_rank
FROM scored
ORDER BY store_id DESC NULLS LAST;
