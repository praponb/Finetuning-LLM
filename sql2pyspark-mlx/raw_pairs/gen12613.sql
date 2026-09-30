WITH latest AS (
    SELECT *, RANK() OVER (PARTITION BY plan, status ORDER BY monthly_fee NULLS FIRST) AS dedup_rank
    FROM subscriptions
    QUALIFY dedup_rank = 1
),
enriched AS (
    SELECT
        *,
        AVG(monthly_fee) OVER w_recent AS running_avg_monthly_fee,
        MIN(monthly_fee) OVER w_recent AS running_min_monthly_fee,
        COUNT(monthly_fee) OVER w_recent AS running_count_monthly_fee,
        MIN(monthly_fee) OVER (PARTITION BY plan) AS plan_min_monthly_fee
    FROM latest
    WHERE dedup_rank = 1
    WINDOW w_recent AS (PARTITION BY plan, status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    SELECT
        id,
        customer_id,
        end_date,
        status,
        dedup_rank,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY dedup_rank DESC, id) AS event_seq,
        SUM(monthly_fee) OVER (PARTITION BY plan, customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY running_avg_monthly_fee DESC, id) AS event_seq_2,
        COUNT(running_count_monthly_fee) OVER (PARTITION BY YEAR(start_date), plan) AS start_date_year_count_running_count_monthly_fee
    FROM enriched
),
with_windows_subscriptions AS (
    SELECT
        id,
        customer_id,
        status,
        dedup_rank,
        cumulative_monthly_fee,
        event_seq_2,
        start_date_year_count_running_count_monthly_fee,
        AVG(event_seq_2) OVER w_ordered AS last7_avg_event_seq_2,
        SUM(event_seq_2) OVER w_ordered AS last7_sum_event_seq_2,
        MIN(event_seq_2) OVER w_ordered AS last7_min_event_seq_2,
        NTILE(5) OVER (PARTITION BY customer_id, status ORDER BY event_seq_2, id) AS event_seq_2_bucket
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY status ORDER BY cumulative_monthly_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN start_date_year_count_running_count_monthly_fee >= 17 THEN 'LARGE' ELSE 'SMALL' END AS start_date_year_count_running_count_monthly_fee_tier,
    status,
    SUM(CASE WHEN status = 'ACTIVE' THEN last7_avg_event_seq_2 ELSE 0 END) AS active_last7_avg_event_seq_2,
    AVG(last7_sum_event_seq_2) AS avg_last7_sum_event_seq_2,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    GROUPING_ID() AS grouping_level
FROM with_windows_subscriptions
GROUP BY GROUPING SETS ((CASE WHEN start_date_year_count_running_count_monthly_fee >= 17 THEN 'LARGE' ELSE 'SMALL' END, status), (CASE WHEN start_date_year_count_running_count_monthly_fee >= 17 THEN 'LARGE' ELSE 'SMALL' END));
