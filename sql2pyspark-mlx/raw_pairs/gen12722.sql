WITH enriched AS (
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        status,
        LEAD(monthly_fee) OVER w_recent next_monthly_fee,
        ROW_NUMBER() OVER w_recent event_seq,
        SUM(monthly_fee) OVER (PARTITION BY DATE_TRUNC('WEEK', start_date), plan ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_monthly_fee
    FROM subscriptions
    WINDOW w_recent AS (PARTITION BY plan ORDER BY start_date, id)
),
base AS (
    SELECT *, event_seq * running_monthly_fee event_seq_times_running_monthly_fee
    FROM enriched
    WHERE customer_id IS NOT NULL
),
enriched_subscriptions AS (
    SELECT
        id,
        end_date,
        status,
        next_monthly_fee,
        event_seq_times_running_monthly_fee,
        SUM(event_seq_times_running_monthly_fee) OVER (PARTITION BY status, customer_id ORDER BY running_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_event_seq_times_running_monthly_fee,
        SUM(running_monthly_fee) OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_running_monthly_fee,
        ROW_NUMBER() OVER (ORDER BY start_date DESC, id) seq_no,
        FIRST_VALUE(running_monthly_fee, TRUE) OVER (PARTITION BY status, customer_id ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_running_monthly_fee
    FROM base
),
filtered AS (
    SELECT *, SUBSTRING(status, 1, 2) status_prefix
    FROM enriched_subscriptions
    WHERE status NOT IN ('PAUSED', 'ACTIVE', 'CANCELLED')
)
SELECT
    id,
    end_date,
    status,
    next_monthly_fee,
    cumulative_running_monthly_fee,
    seq_no,
    first_known_running_monthly_fee,
    status_prefix,
    CASE WHEN status RLIKE '^(EXPIRED|CANCELLED)$' THEN 'GROUP_A' ELSE 'CORE' END AS status_zone
FROM filtered
ORDER BY seq_no DESC, end_date
