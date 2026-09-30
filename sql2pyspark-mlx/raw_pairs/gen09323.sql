WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), started_at) / 7 AS weeks_since_started_at,
        CASE WHEN country IN ('UK', 'DE') THEN 'Core' ELSE 'Other' END AS country_group
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-05-03 19:00:00'
      AND device NOT IN ('mobile', 'desktop')
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.browser,
        s.started_at,
        s.country,
        top1.duration_sec
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_last a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.started_at >= sub.start_date
       AND a.started_at < sub.end_date + INTERVAL 14 DAYS
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        started_at,
        duration_sec,
        subscription_id,
        plan,
        ROUND(subscription_id / SUM(subscription_id) OVER (PARTITION BY plan), 4) AS subscription_id_share,
        SUM(monthly_fee) OVER (PARTITION BY YEAR(started_at) ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS monthly_fee_sum_30d
    FROM matched
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        duration_sec,
        subscription_id,
        subscription_id_share,
        monthly_fee_sum_30d
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY duration_sec DESC, id, subscription_id) AS row_num
        FROM enriched
    ) x
    WHERE row_num = 1
),
enriched_sessions AS (
    -- Ranking and gap features
    SELECT
        id,
        duration_sec,
        subscription_id,
        NTILE(5) OVER w_recent AS subscription_id_share_bucket,
        ROW_NUMBER() OVER (ORDER BY monthly_fee_sum_30d DESC, id, subscription_id) AS seq_no,
        LEAD(duration_sec, 1, 0) OVER w_recent AS next_duration_sec
    FROM deduped
    WHERE monthly_fee_sum_30d BETWEEN 338 AND 1035
    WINDOW w_recent AS (ORDER BY subscription_id_share, id, subscription_id)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN duration_sec >= 461 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
        ROUND(AVG(subscription_id_share_bucket), 2) AS avg_subscription_id_share_bucket,
        ROUND(AVG(seq_no), 2) AS avg_seq_no,
        APPROX_PERCENTILE(seq_no, 0.5) AS seq_no_p50,
        ROUND(AVG(next_duration_sec), 2) AS avg_next_duration_sec,
        APPROX_PERCENTILE(subscription_id, 0.9) AS subscription_id_p90
    FROM enriched_sessions
    GROUP BY CASE WHEN duration_sec >= 461 THEN 'HIGH' ELSE 'LOW' END
    HAVING seq_no_p50 > 0
)
SELECT
    duration_sec_tier,
    avg_next_duration_sec,
    subscription_id_p90
FROM summary
ORDER BY duration_sec_tier NULLS LAST, avg_next_duration_sec
LIMIT 10
