WITH cleaned AS (
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        DATEDIFF(CURRENT_DATE(), started_at) / 365 AS years_since_started_at,
        CASE WHEN country IN ('JP', 'TH') THEN 'Tier 1' ELSE 'Other' END AS country_group,
        COALESCE(country, 'OTHER') AS resolved_country
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-08-17 22:00:00'
),
latest AS (
    SELECT
        id,
        customer_id,
        started_at,
        country,
        years_since_started_at,
        country_group
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY browser, country ORDER BY years_since_started_at DESC, id) AS latest_rank
        FROM cleaned
    ) ranked
    WHERE latest_rank = 1
),
enriched AS (
    SELECT
        id,
        customer_id,
        started_at,
        country_group,
        SUM(years_since_started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_years_since_started_at,
        MIN(years_since_started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_years_since_started_at,
        AVG(years_since_started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_years_since_started_at,
        PERCENT_RANK() OVER (PARTITION BY country, customer_id ORDER BY years_since_started_at DESC NULLS FIRST) AS years_since_started_at_percent_rank
    FROM latest
    WHERE country IN ('US', 'UK', 'SG')
),
latest_match AS (
    SELECT
        s.*,
        top1.event_type,
        top1.duration_sec
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.event_type, c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    customer_id,
    started_at,
    running_min_years_since_started_at,
    running_avg_years_since_started_at,
    years_since_started_at_percent_rank,
    event_type,
    duration_sec,
    CASE WHEN event_type RLIKE '^(search|click)$' THEN 'EXTENDED' WHEN event_type RLIKE '^(page_view|add_to_cart)$' THEN 'GROUP_B' ELSE 'CORE' END AS event_type_zone
FROM latest_match
ORDER BY event_type_zone
