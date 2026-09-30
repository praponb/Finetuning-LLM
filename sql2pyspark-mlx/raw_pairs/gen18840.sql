WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TO_DATE(started_at) AS started_at_date,
        LENGTH(device) AS device_length
    FROM sessions
    WHERE started_at >= DATE '2023-07-11'
      AND customer_id IS NOT NULL
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT device, country
    FROM filtered
    INTERSECT
    SELECT device, country
    FROM sessions
    WHERE browser IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.device = k.device AND s.country = k.country
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('YEAR', started_at_date), country ORDER BY device_length NULLS LAST) AS device_length_cume_dist,
        DENSE_RANK() OVER (PARTITION BY device ORDER BY device_length) AS device_length_dense_rank
    FROM verified
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time >= s.started_at
        )
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.device,
        e.browser,
        e.started_at,
        e.device_length_dense_rank,
        b.signup_date,
        b.segment,
        b.city
    FROM kept e
    INNER JOIN customers b
        ON e.customer_id = b.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_dims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.session_id IN (14, 23)
        )
)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.started_at,
    s.device_length_dense_rank,
    s.signup_date,
    s.segment,
    (SELECT MIN(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS min_account_balance
FROM not_flagged s
ORDER BY customer_id;
