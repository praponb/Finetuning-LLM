WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(country, 1, 3) AS country_prefix
    FROM sessions
    WHERE started_at >= TO_DATE('2023-03-06')
      AND customer_id IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id ASC) AS rank_in_group
        FROM valid
    ) ranked
    WHERE rank_in_group = 1
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.web_event_event_type,
        x.web_event_event_time
    FROM deduped s
    JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type, c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.browser,
        e.started_at,
        e.web_event_event_type,
        c.city,
        c.email,
        c.signup_date
    FROM with_latest e
    LEFT JOIN customers c
        ON e.customer_id = c.id
    WHERE c.age <= 44
),
matched_keys AS (
    -- Set intersection on keys
    SELECT browser
    FROM joined
    INTERSECT ALL
    SELECT browser
    FROM sessions
    WHERE device = 'tablet'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    JOIN matched_keys k ON s.browser = k.browser
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.web_event_event_type,
        src.city,
        src.signup_date,
        p.credit_limit,
        p.city AS customer_city,
        p.age
    FROM kept_rows src
    LEFT JOIN customers p
        ON src.customer_id <=> p.id
)
SELECT
    s.customer_id,
    s.city,
    s.signup_date,
    s.credit_limit,
    CASE WHEN city RLIKE '^(Chiang Mai|Bangkok)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS city_zone,
    (SELECT MIN(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM enriched s;
