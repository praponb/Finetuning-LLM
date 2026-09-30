WITH scored AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        status,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 4) AS monthly_fee_share,
        ROW_NUMBER() OVER w_recent AS event_seq,
        NTILE(4) OVER w_recent_2 AS monthly_fee_bucket,
        LAG(monthly_fee, 1) OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee, id) AS prev_monthly_fee,
        ROW_NUMBER() OVER w_ordered AS event_seq_2
    FROM subscriptions
    WINDOW w_recent AS (PARTITION BY MONTH(start_date), customer_id ORDER BY start_date, id), w_recent_2 AS (PARTITION BY customer_id ORDER BY monthly_fee DESC, id), w_ordered AS (ORDER BY monthly_fee, id)
),
with_window AS (
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee
    FROM scored s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.start_date BETWEEN sub.start_date AND sub.end_date
),
matched AS (
    SELECT
        e.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan_2,
        sub.monthly_fee AS subscription_monthly_fee
    FROM with_window e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.start_date BETWEEN sub.start_date AND sub.end_date
),
scored_subscriptions AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY monthly_fee_bucket NULLS LAST) AS monthly_fee_bucket_percent_rank,
        ROUND(prev_monthly_fee / SUM(prev_monthly_fee) OVER (PARTITION BY subscription_plan), 4) AS prev_monthly_fee_share,
        SUM(monthly_fee) OVER (PARTITION BY subscription_plan_2, subscription_plan ORDER BY start_date, id, subscription_id, subscription_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee
    FROM matched
),
with_windows AS (
    SELECT
        id,
        plan,
        status,
        monthly_fee_share,
        event_seq,
        prev_monthly_fee,
        subscription_id,
        monthly_fee,
        subscription_id_2,
        monthly_fee_bucket_percent_rank,
        cumulative_monthly_fee,
        MIN(subscription_id) OVER w_part AS running_min_subscription_id,
        AVG(subscription_id) OVER w_part AS running_avg_subscription_id,
        MAX(subscription_id) OVER w_part AS running_max_subscription_id
    FROM scored_subscriptions
    WINDOW w_part AS (PARTITION BY subscription_plan_2 ORDER BY monthly_fee_bucket, id, subscription_id, subscription_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY monthly_fee_bucket_percent_rank DESC NULLS FIRST, id, subscription_id, subscription_id_2) AS latest_rank
        FROM with_windows
    ) t
    WHERE latest_rank = 1
),
cleaned AS (
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM latest
    WHERE running_min_subscription_id <= 6.9
)
SELECT
    id,
    plan,
    status,
    monthly_fee_share,
    prev_monthly_fee,
    subscription_id,
    monthly_fee,
    subscription_id_2,
    running_min_subscription_id,
    CASE WHEN plan RLIKE '^(enterprise|premium)$' THEN 'CORE' WHEN plan RLIKE '^(basic|standard)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS plan_zone
FROM cleaned
