WITH filtered AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        DAYOFWEEK(started_at) AS started_at_dayofweek,
        COALESCE(country, 'DIRECT') AS resolved_country
    FROM sessions
    WHERE customer_id IS NOT NULL
),
valid AS (
    SELECT *, CONCAT(device, '/', country) AS device_country
    FROM filtered
    WHERE browser IS DISTINCT FROM 'Safari'
      AND country IS NOT NULL
),
with_last AS (
    SELECT
        s.id,
        s.customer_id,
        s.country,
        s.device_country,
        latest.web_event_page,
        latest.web_event_duration_sec
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.page AS web_event_page, c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_dims AS (
    SELECT
        s.id,
        s.customer_id,
        s.country,
        s.device_country,
        s.web_event_page,
        s.web_event_duration_sec,
        d.city,
        d.credit_limit
    FROM with_last s
    LEFT JOIN customers d
        ON s.customer_id = d.id
    WHERE d.age BETWEEN 40 AND 63
),
kept AS (
    SELECT s.*
    FROM with_dims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.duration_sec BETWEEN 352 AND 394
        )
),
daily AS (
    SELECT
        customer_id,
        CASE WHEN web_event_duration_sec >= 472 THEN 'PREMIUM' ELSE 'STANDARD' END AS web_event_duration_sec_tier,
        SIZE(COLLECT_SET(city)) AS num_cities,
        SIZE(COLLECT_SET(city)) AS num_cities_2,
        MIN(credit_limit) AS min_credit_limit,
        SUM(credit_limit) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_total_credit_limit
    FROM kept
    GROUP BY customer_id, web_event_duration_sec_tier
    HAVING min_credit_limit > 0
)
SELECT
    customer_id,
    web_event_duration_sec_tier,
    num_cities,
    chiang_mai_total_credit_limit
FROM daily
ORDER BY num_cities DESC, chiang_mai_total_credit_limit;
