WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC NULLS LAST, id) AS row_num
        FROM subscriptions
    ) x
    WHERE row_num = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date DESC, id) AS rn
    FROM deduped
    QUALIFY rn = 1
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.plan,
        a.end_date,
        a.monthly_fee,
        a.rn,
        d.credit_limit,
        d.segment
    FROM latest a
    LEFT JOIN customers d
        ON a.customer_id <=> d.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        segment,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY MONTH(end_date), plan), 4) AS monthly_fee_share,
        AVG(monthly_fee) OVER (PARTITION BY segment, customer_id ORDER BY monthly_fee, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_2,
        SUM(monthly_fee) OVER w_recent AS running_monthly_fee,
        LAST_VALUE(segment, TRUE) OVER (PARTITION BY plan ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_segment,
        LAST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY YEAR(end_date) ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit
    FROM with_dims
    WHERE credit_limit >= 6300
    WINDOW w_recent AS (PARTITION BY customer_id, plan ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, segment ORDER BY running_monthly_fee DESC, id) AS dedup_rank
    FROM metrics
    QUALIFY dedup_rank = 1
),
ranked_subscriptions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY plan ORDER BY running_monthly_fee) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
),
ranked_subscriptions_2 AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY dedup_rank, id) AS latest_rank_2
        FROM ranked_subscriptions
    ) t
    WHERE latest_rank_2 = 1
)
-- Ranking and gap features
SELECT
    id,
    segment,
    monthly_fee_share,
    monthly_fee_avg_2,
    running_monthly_fee,
    last_known_credit_limit,
    latest_rank,
    LEAD(monthly_fee_share, 2) OVER (PARTITION BY segment ORDER BY monthly_fee_avg_2, id) AS next_monthly_fee_share,
    SUM(running_monthly_fee) OVER w_ordered AS cumulative_running_monthly_fee,
    ROW_NUMBER() OVER (ORDER BY latest_rank_2 DESC, id) AS seq_no,
    FIRST_VALUE(segment, TRUE) OVER w_ordered AS first_known_segment,
    LAST_VALUE(segment) IGNORE NULLS OVER w_grp AS last_known_segment_2
FROM ranked_subscriptions_2
WINDOW w_ordered AS (ORDER BY last_known_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY plan ORDER BY running_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
