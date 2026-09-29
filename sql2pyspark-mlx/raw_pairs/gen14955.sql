WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC NULLS LAST, id) AS rn
        FROM subscriptions
    ) t
    WHERE rn = 1
),
deduped_subscriptions AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee) AS latest_rank
    FROM deduped
    QUALIFY latest_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        rn,
        LEAD(monthly_fee) OVER (PARTITION BY customer_id ORDER BY rn, id) AS next_monthly_fee,
        LAG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY rn, id) AS prev_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY rn, id) AS event_seq,
        CUME_DIST() OVER (PARTITION BY status, customer_id ORDER BY monthly_fee) AS monthly_fee_cume_dist
    FROM deduped_subscriptions
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.name,
        d.segment,
        d.credit_limit
    FROM enriched s
    LEFT JOIN customers d
        ON s.customer_id = d.id
    WHERE d.segment = 'Corporate'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN credit_limit >= 17900 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_tier,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list,
        COUNT_IF(segment = 'SMB') AS smb_events,
        SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
        SUM(credit_limit) AS total_credit_limit,
        SIZE(COLLECT_SET(segment)) AS num_segments,
        COUNT(*) FILTER (WHERE segment = 'Consumer') AS consumer_rows
    FROM enriched_subscriptions
    GROUP BY ROLLUP (CASE WHEN credit_limit >= 17900 THEN 'HIGH' ELSE 'LOW' END)
)
SELECT
    credit_limit_tier,
    segment_list,
    num_segments,
    consumer_rows
FROM stats
ORDER BY credit_limit_tier DESC NULLS LAST
