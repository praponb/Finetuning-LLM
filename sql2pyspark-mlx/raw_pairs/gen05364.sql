WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(cost, 1) OVER (ORDER BY shipped_date, id) AS prev_cost,
        FIRST_VALUE(carrier, TRUE) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_carrier,
        SUM(cost) OVER (PARTITION BY YEAR(delivered_date), order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
    FROM shipments
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        status,
        prev_cost,
        first_known_carrier,
        running_cost,
        COALESCE(prev_cost, 0) AS prev_cost_filled,
        ROUND(running_cost, 2) AS running_cost_rounded
    FROM scored
    WHERE shipped_date >= DATE '2023-03-21'
      AND carrier IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY prev_cost, id) AS rn
    FROM base
    QUALIFY rn = 1
),
scored_shipments AS (
    -- Running and moving aggregates
    SELECT
        id,
        delivered_date,
        first_known_carrier,
        rn,
        AVG(running_cost_rounded) OVER (PARTITION BY DATE_TRUNC('MONTH', delivered_date), status ORDER BY prev_cost, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS running_cost_rounded_avg_6_prior,
        LAG(prev_cost_filled, 1) OVER (PARTITION BY status ORDER BY delivered_date, id) AS prev_prev_cost_filled,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('month', delivered_date), status ORDER BY cost) AS cost_cume_dist
    FROM ranked
    WHERE prev_cost <= 47
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        delivered_date,
        rn,
        running_cost_rounded_avg_6_prior,
        cost_cume_dist,
        STDDEV_SAMP(running_cost_rounded_avg_6_prior) OVER (PARTITION BY YEAR(delivered_date)) AS delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior,
        SUM(running_cost_rounded_avg_6_prior) OVER (ORDER BY prev_prev_cost_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_cost_rounded_avg_6_prior
    FROM scored_shipments
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('QUARTER', delivered_date) AS quarter_start,
        CASE WHEN delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior >= 78 THEN 'LARGE' ELSE 'SMALL' END AS delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior_tier,
        PERCENTILE_APPROX(delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior, 0.95) AS delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior_p95,
        APPROX_PERCENTILE(cumulative_running_cost_rounded_avg_6_prior, 0.5) AS cumulative_running_cost_rounded_avg_6_prior_p50,
        MAX(delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior) AS max_delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior,
        AVG(cost_cume_dist) AS avg_cost_cume_dist,
        PERCENTILE_APPROX(cost_cume_dist, 0.95) AS cost_cume_dist_p95
    FROM enriched
    GROUP BY 1, 2
)
SELECT
    quarter_start,
    delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior_tier,
    delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior_p95,
    cumulative_running_cost_rounded_avg_6_prior_p50,
    max_delivered_date_year_stddev_samp_running_cost_rounded_avg_6_prior,
    avg_cost_cume_dist
FROM summary
