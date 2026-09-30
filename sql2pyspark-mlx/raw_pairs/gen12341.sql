WITH base AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        CASE WHEN browser IN ('Safari', 'Edge') THEN 'Group A' ELSE 'Other' END browser_group,
        CASE WHEN device IN ('mobile', 'desktop') THEN 'Tier 1' ELSE 'Other' END device_group,
        COALESCE(browser, 'UNKNOWN') browser_filled
    FROM sessions
    WHERE started_at > TO_TIMESTAMP('2023-05-27 15:00:00')
      AND country IS NOT NULL
),
attributed AS (
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM base e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.started_at BETWEEN sub.start_date AND sub.end_date
),
enriched AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY MONTH(started_at), device ORDER BY subscription_id, id, subscription_id) subscription_id_bucket,
        LAST_VALUE(browser, TRUE) OVER w_ordered last_known_browser,
        AVG(monthly_fee) OVER (PARTITION BY plan ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) monthly_fee_avg_5,
        NTILE(5) OVER (ORDER BY monthly_fee DESC, id, subscription_id) monthly_fee_bucket
    FROM attributed
    WHERE monthly_fee BETWEEN 180 AND 220
    WINDOW w_ordered AS (PARTITION BY YEAR(started_at), country ORDER BY started_at, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_sessions AS (
    SELECT
        s.*,
        b.country AS customer_country,
        b.credit_limit,
        b.city
    FROM enriched s
    INNER JOIN customers b
        ON s.customer_id <=> b.id
)
SELECT
    CASE WHEN monthly_fee >= 207 THEN 'LARGE' ELSE 'SMALL' END AS monthly_fee_tier,
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    CASE WHEN monthly_fee_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_bucket_tier,
    SUM(monthly_fee_avg_5) FILTER (WHERE customer_country = 'US') AS us_total_monthly_fee_avg_5,
    COUNT(CASE WHEN device = 'mobile' THEN 1 END) AS mobile_count,
    SUM(subscription_id) AS total_subscription_id
FROM enriched_sessions
GROUP BY CUBE (CASE WHEN monthly_fee >= 207 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', started_at), CASE WHEN monthly_fee_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END)
HAVING total_subscription_id >= 0
ORDER BY quarter_start DESC
