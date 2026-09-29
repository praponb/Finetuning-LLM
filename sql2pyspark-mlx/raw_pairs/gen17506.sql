WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(country, 'Unknown') AS country_filled
    FROM sessions
    WHERE HOUR(started_at) BETWEEN 11 AND 15
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time <= s.started_at
        )
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM eligible a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.started_at >= sub.start_date
       AND a.started_at < sub.end_date + INTERVAL 30 DAYS
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.event_type,
        top1.page
    FROM matched s
    JOIN LATERAL (
            SELECT c.event_type, c.page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT browser
    FROM latest_match
    INTERSECT ALL
    SELECT browser
    FROM sessions
    WHERE customer_id IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match s
    JOIN remaining_keys k ON s.browser = k.browser
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', started_at) AS activity_month,
        CASE WHEN subscription_id >= 10 THEN 'PREMIUM' ELSE 'STANDARD' END AS subscription_id_tier,
        MIN(monthly_fee) AS min_monthly_fee,
        PERCENTILE_APPROX(monthly_fee, 0.9) AS monthly_fee_p90,
        SIZE(COLLECT_SET(page)) AS num_pages,
        SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
        SUM(monthly_fee) FILTER (WHERE page = '/search') AS search_total_monthly_fee
    FROM reconciled
    GROUP BY DATE_TRUNC('MONTH', started_at), CASE WHEN subscription_id >= 10 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING SUM(monthly_fee) FILTER (WHERE page = '/search') > 0
)
SELECT
    activity_month,
    subscription_id_tier,
    min_monthly_fee,
    monthly_fee_p90,
    browser_list
FROM totals;
