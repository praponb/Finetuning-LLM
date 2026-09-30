WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        status,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_monthly_fee,
        COUNT(monthly_fee) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_monthly_fee
    FROM subscriptions
),
with_windows_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(monthly_fee) OVER (PARTITION BY plan ORDER BY running_sum_monthly_fee, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_3_prior,
        RANK() OVER (PARTITION BY plan ORDER BY running_avg_monthly_fee DESC) AS running_avg_monthly_fee_rank,
        ROW_NUMBER() OVER (PARTITION BY MONTH(end_date), plan ORDER BY monthly_fee DESC, id) AS seq_no,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id, status), 4) AS monthly_fee_share
    FROM with_windows
    WHERE running_avg_monthly_fee >= 90
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM with_windows_subscriptions src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.end_date >= sub.start_date
       AND src.end_date <= DATE_ADD(sub.end_date, 10)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(seq_no) IGNORE NULLS OVER (ORDER BY seq_no, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_seq_no,
        LAG(running_avg_monthly_fee_rank, 1) OVER (PARTITION BY DATE_TRUNC('YEAR', end_date), subscription_plan ORDER BY monthly_fee, id, subscription_id) AS prev_running_avg_monthly_fee_rank,
        SUM(running_count_monthly_fee) OVER (PARTITION BY plan, subscription_plan ORDER BY seq_no, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_count_monthly_fee
    FROM with_window
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY prev_running_avg_monthly_fee_rank DESC NULLS FIRST, id, subscription_id) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
)
SELECT
    s.id,
    s.customer_id,
    s.plan,
    s.monthly_fee,
    s.status,
    s.running_avg_monthly_fee,
    s.running_sum_monthly_fee,
    s.monthly_fee_avg_3_prior,
    s.running_avg_monthly_fee_rank,
    CASE WHEN status RLIKE '^(PAUSED|EXPIRED)$' THEN 'CORE' WHEN status RLIKE '^(ACTIVE|CANCELLED)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS status_zone,
    (SELECT MAX(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS max_subscription_monthly_fee
FROM deduped s
ORDER BY running_avg_monthly_fee_rank, status_zone DESC
