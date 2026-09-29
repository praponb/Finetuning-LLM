WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS latest_rank
    FROM store_sales
    QUALIFY latest_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        CORR(units, revenue) OVER w_running corr_units_revenue,
        MIN(revenue) OVER (PARTITION BY DATE_TRUNC('WEEK', sale_date), store_id) sale_date_period_min_revenue,
        SUM(revenue) OVER (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_revenue,
        SUM(revenue) OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_revenue,
        LAG(units, 2) OVER (ORDER BY sale_date, id) prev_units
    FROM ranked
    WHERE product_id = 10
    WINDOW w_running AS (PARTITION BY store_id)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY cumulative_revenue NULLS LAST, id) = 1
),
scored_store_sales AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        sale_date,
        corr_units_revenue,
        sale_date_period_min_revenue,
        cumulative_revenue,
        running_revenue,
        LAG(prev_units, 2) OVER (PARTITION BY store_id ORDER BY corr_units_revenue, id) prev_prev_units,
        LAG(prev_units, 1) OVER (PARTITION BY store_id ORDER BY revenue, id) prev_prev_units_2,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY cumulative_revenue DESC) cumulative_revenue_percent_rank,
        CUME_DIST() OVER (ORDER BY cumulative_revenue DESC NULLS LAST) cumulative_revenue_cume_dist
    FROM latest
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(prev_prev_units_2) IGNORE NULLS OVER (ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_prev_prev_units_2,
        SUM(sale_date_period_min_revenue) OVER (ORDER BY cumulative_revenue_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_sale_date_period_min_revenue,
        LEAD(cumulative_revenue, 1) OVER (ORDER BY corr_units_revenue, id) next_cumulative_revenue
    FROM scored_store_sales
)
-- Window metrics over several different partitions
SELECT
    id,
    product_id,
    sale_date,
    corr_units_revenue,
    cumulative_revenue,
    running_revenue,
    prev_prev_units_2,
    cumulative_revenue_percent_rank,
    cumulative_revenue_cume_dist,
    first_known_prev_prev_units_2,
    next_cumulative_revenue,
    ROUND(cumulative_revenue / SUM(cumulative_revenue) OVER (PARTITION BY product_id), 4) cumulative_revenue_share,
    SUM(sale_date_period_min_revenue) OVER (ORDER BY cumulative_revenue_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sale_date_period_min_revenue
FROM metrics;
