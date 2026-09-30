WITH enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY browser, customer_id ORDER BY started_at, id) AS event_seq,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country
    FROM sessions
)
SELECT
    s.id,
    s.customer_id,
    s.started_at,
    s.country,
    s.event_seq,
    s.first_known_country,
    CASE WHEN country RLIKE '^(US|SG)$' THEN 'OTHER_ZONE' WHEN country RLIKE '^(TH|JP)$' THEN 'CORE' ELSE 'PRIORITY' END AS country_zone,
    (SELECT MIN(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM enriched s
ORDER BY started_at DESC NULLS LAST;
