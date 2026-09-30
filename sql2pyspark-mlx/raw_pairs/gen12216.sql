WITH enriched AS (
    SELECT
        src.*,
        d.email,
        d.name
    FROM subscriptions src
    JOIN customers d
        ON src.customer_id = d.id
),
enriched_subscriptions AS (
    SELECT
        *,
        MIN(monthly_fee) OVER w_part AS running_min_monthly_fee,
        SUM(monthly_fee) OVER w_part AS running_sum_monthly_fee,
        MAX(monthly_fee) OVER w_part AS running_max_monthly_fee
    FROM enriched
    WINDOW w_part AS (PARTITION BY customer_id ORDER BY monthly_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY monthly_fee DESC, id ASC) AS event_seq,
        LAG(running_max_monthly_fee, 1) OVER (PARTITION BY status ORDER BY running_sum_monthly_fee ASC, id ASC) AS prev_running_max_monthly_fee,
        LEAD(running_min_monthly_fee, 1) OVER (PARTITION BY customer_id ORDER BY running_max_monthly_fee ASC, id ASC) AS next_running_min_monthly_fee
    FROM enriched_subscriptions
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee ASC NULLS LAST, id ASC) AS row_num
    FROM scored
    QUALIFY row_num = 1
),
metrics AS (
    SELECT
        id,
        customer_id,
        status,
        running_min_monthly_fee,
        event_seq,
        FIRST_VALUE(next_running_min_monthly_fee, TRUE) OVER (PARTITION BY status ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_running_min_monthly_fee,
        LAST_VALUE(next_running_min_monthly_fee) IGNORE NULLS OVER (ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_running_min_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date ASC, id ASC) AS row_seq
    FROM ranked
    WHERE customer_id = 37
),
enriched_subscriptions_2 AS (
    SELECT
        src.id,
        src.customer_id,
        src.status,
        src.event_seq,
        src.last_known_next_running_min_monthly_fee,
        b.credit_limit
    FROM metrics src
    JOIN customers b
        ON src.customer_id = b.id
)
SELECT
    CASE WHEN event_seq >= 7 THEN 'HIGH' ELSE 'LOW' END AS event_seq_tier,
    ROUND(AVG(event_seq), 2) AS avg_event_seq,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(*) AS row_count,
    COUNT_IF(status = 'ACTIVE') AS active_events,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list_2
FROM enriched_subscriptions_2
GROUP BY event_seq_tier
