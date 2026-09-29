WITH cleaned AS (
    SELECT
        *,
        LAST_DAY(end_date) AS end_date_month_end,
        LOWER(status) AS status_lower
    FROM subscriptions
    WHERE monthly_fee >= 220
),
metrics AS (
    SELECT
        *,
        SUM(monthly_fee) OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee,
        NTILE(5) OVER (PARTITION BY YEAR(start_date), status ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket,
        CUME_DIST() OVER (PARTITION BY status, plan ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_cume_dist
    FROM cleaned
),
deduped AS (
    SELECT *
    FROM metrics
    QUALIFY RANK() OVER (PARTITION BY plan ORDER BY monthly_fee DESC) = 1
),
enriched AS (
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        end_date_month_end,
        monthly_fee_bucket,
        MIN(monthly_fee_cume_dist) OVER (PARTITION BY YEAR(start_date), plan) AS start_date_year_min_monthly_fee_cume_dist,
        PERCENT_RANK() OVER w_ordered AS monthly_fee_cume_dist_percent_rank,
        SUM(monthly_fee_bucket) OVER w_recent AS cumulative_monthly_fee_bucket
    FROM deduped
    WINDOW w_ordered AS (PARTITION BY status ORDER BY monthly_fee_cume_dist DESC NULLS LAST), w_recent AS (PARTITION BY status ORDER BY monthly_fee_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    SELECT
        *,
        LENGTH(plan) AS plan_length,
        CEIL(cumulative_monthly_fee_bucket) AS cumulative_monthly_fee_bucket_whole
    FROM enriched
    WHERE end_date > DATE '2023-10-10'
)
SELECT
    CASE WHEN start_date_year_min_monthly_fee_cume_dist >= 1 THEN 'LARGE' ELSE 'SMALL' END AS start_date_year_min_monthly_fee_cume_dist_tier,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    STDDEV_SAMP(monthly_fee) AS monthly_fee_stddev,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    APPROX_PERCENTILE(monthly_fee, 0.9) AS monthly_fee_p90,
    COUNT(*) FILTER (WHERE plan = 'standard') AS standard_rows
FROM valid
GROUP BY CASE WHEN start_date_year_min_monthly_fee_cume_dist >= 1 THEN 'LARGE' ELSE 'SMALL' END
HAVING monthly_fee_p90 > 0
