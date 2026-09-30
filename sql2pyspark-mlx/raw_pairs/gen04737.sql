WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee) AS dedup_rank
        FROM subscriptions
    ) x
    WHERE dedup_rank = 1
),
ranked_subscriptions AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) AS rank_in_group
        FROM ranked
    ) t
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(monthly_fee) OVER (PARTITION BY status ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_monthly_fee,
        MIN(monthly_fee) OVER (PARTITION BY status ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_monthly_fee
    FROM ranked_subscriptions
    WHERE start_date <= '2024-12-01'
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.start_date,
        e.monthly_fee,
        e.status,
        e.dedup_rank,
        e.rank_in_group,
        e.running_avg_monthly_fee,
        b.age
    FROM with_windows e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
daily AS (
    -- Grouped metrics
    SELECT
        status,
        PERCENTILE_APPROX(running_avg_monthly_fee, 0.5) AS running_avg_monthly_fee_p50,
        SUM(CASE WHEN status = 'EXPIRED' THEN age ELSE 0 END) AS expired_age
    FROM with_dims
    GROUP BY status
)
SELECT
    status,
    expired_age,
    CASE WHEN status RLIKE '^(PAUSED|CANCELLED)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS status_zone
FROM daily;
