WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        CONCAT_WS('-', device, browser) AS device_browser_key
    FROM sessions
    WHERE started_at >= TO_TIMESTAMP('2023-02-20 07:00:00')
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time >= s.started_at
        )
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        b.email,
        b.segment,
        b.country AS customer_country
    FROM kept a
    LEFT JOIN customers b
        ON a.customer_id = b.id
    WHERE b.country NOT IN ('US', 'TH')
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.city,
        d.email AS customer_email
    FROM with_dims a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.event_time
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.email,
    s.segment,
    s.customer_country,
    s.customer_email,
    (SELECT MAX(x.amount) FROM orders x WHERE x.customer_id = s.customer_id) AS max_order_amount
FROM latest_match s
ORDER BY id;
