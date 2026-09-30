WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        GREATEST(revenue, units) AS greatest_revenue_units
    FROM store_sales
    WHERE product_id IN (7, 9)
),
metrics AS (
    SELECT
        *,
        AVG(revenue) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS revenue_avg_6_prior,
        AVG(revenue) OVER (PARTITION BY YEAR(sale_date), product_id ORDER BY greatest_revenue_units, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS revenue_avg_2,
        LAG(units, 2) OVER w_hist AS prev_units
    FROM valid
    WINDOW w_hist AS (PARTITION BY YEAR(sale_date) ORDER BY units, id)
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY prev_units, id) AS latest_rank
        FROM metrics
    ) t
    WHERE latest_rank = 1
),
enriched AS (
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue_avg_6_prior,
        prev_units,
        MAX(revenue_avg_2) OVER w_running AS running_max_revenue_avg_2,
        COUNT(revenue_avg_2) OVER w_running AS running_count_revenue_avg_2,
        SUM(revenue_avg_2) OVER w_running AS running_sum_revenue_avg_2,
        PERCENT_RANK() OVER w_grp AS revenue_avg_6_prior_percent_rank
    FROM latest
    WHERE store_id IN (2, 3, 5)
    WINDOW w_running AS (PARTITION BY store_id, product_id ORDER BY greatest_revenue_units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY product_id ORDER BY revenue_avg_6_prior DESC NULLS FIRST)
),
scored AS (
    SELECT
        id,
        sale_date,
        units,
        revenue_avg_6_prior,
        running_max_revenue_avg_2,
        running_sum_revenue_avg_2,
        revenue_avg_6_prior_percent_rank,
        FIRST_VALUE(running_count_revenue_avg_2, TRUE) OVER (ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_count_revenue_avg_2,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', sale_date), store_id ORDER BY revenue_avg_6_prior_percent_rank, id) AS event_seq
    FROM enriched
)
SELECT
    id,
    sale_date,
    NTILE(10) OVER (ORDER BY event_seq, id) AS event_seq_bucket,
    NTILE(10) OVER (ORDER BY revenue_avg_6_prior_percent_rank DESC, id) AS revenue_avg_6_prior_percent_rank_bucket
FROM scored
