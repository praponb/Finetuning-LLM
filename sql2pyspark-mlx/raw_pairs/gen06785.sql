WITH enriched AS (
    SELECT
        *,
        SUM(monthly_fee) OVER (PARTITION BY plan) plan_sum_monthly_fee,
        CUME_DIST() OVER (ORDER BY monthly_fee DESC NULLS LAST) monthly_fee_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date, id) event_seq,
        MAX(monthly_fee) OVER (PARTITION BY MONTH(end_date), status) end_date_month_max_monthly_fee
    FROM subscriptions
    WHERE monthly_fee BETWEEN 100 AND 220
),
matched AS (
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM enriched e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.start_date >= sub.start_date
       AND e.start_date < sub.end_date + INTERVAL 30 DAYS
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC NULLS LAST, id, subscription_id) AS row_num
    FROM matched
    QUALIFY row_num = 1
),
metrics AS (
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        plan_sum_monthly_fee,
        subscription_id,
        row_num,
        SUM(event_seq) OVER w_hist status_sum_event_seq,
        ROW_NUMBER() OVER (PARTITION BY MONTH(start_date) ORDER BY row_num, id, subscription_id) seq_no,
        LEAD(monthly_fee, 1) OVER (PARTITION BY subscription_plan ORDER BY monthly_fee_cume_dist, id, subscription_id) next_monthly_fee,
        ROUND(plan_sum_monthly_fee / SUM(plan_sum_monthly_fee) OVER (PARTITION BY status, customer_id), 4) plan_sum_monthly_fee_share
    FROM latest
    WINDOW w_hist AS (PARTITION BY status)
),
with_windows AS (
    SELECT
        id,
        plan,
        end_date,
        subscription_id,
        row_num,
        next_monthly_fee,
        ROUND(status_sum_event_seq / SUM(status_sum_event_seq) OVER (PARTITION BY customer_id, plan), 4) status_sum_event_seq_share,
        AVG(next_monthly_fee) OVER (PARTITION BY MONTH(end_date), plan ORDER BY subscription_id, id, subscription_id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) next_monthly_fee_avg_5,
        SUM(seq_no) OVER (PARTITION BY YEAR(end_date), plan ORDER BY plan_sum_monthly_fee_share, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_seq_no,
        SUM(seq_no) OVER (ORDER BY subscription_id, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_seq_no_2
    FROM metrics
),
base AS (
    SELECT
        id,
        plan,
        end_date,
        subscription_id,
        row_num,
        next_monthly_fee,
        status_sum_event_seq_share,
        cumulative_seq_no,
        cumulative_seq_no_2,
        cumulative_seq_no - row_num cumulative_seq_no_minus_row_num
    FROM with_windows
    WHERE cumulative_seq_no_2 IS NULL
),
with_windows_subscriptions AS (
    SELECT
        id,
        plan,
        subscription_id,
        next_monthly_fee,
        SUM(next_monthly_fee) OVER (ORDER BY status_sum_event_seq_share, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_next_monthly_fee,
        RANK() OVER w_part cumulative_seq_no_rank
    FROM base
    WINDOW w_part AS (PARTITION BY plan ORDER BY cumulative_seq_no DESC NULLS FIRST)
)
SELECT
    id,
    plan,
    subscription_id,
    running_next_monthly_fee,
    cumulative_seq_no_rank
FROM with_windows_subscriptions
ORDER BY id DESC NULLS LAST, subscription_id DESC
