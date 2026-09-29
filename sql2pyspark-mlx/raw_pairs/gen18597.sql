WITH joined AS (
    -- Attach customers attributes
    SELECT s.*, c.email
    FROM sessions s
    INNER JOIN customers c
        ON s.customer_id = c.id
    WHERE YEAR(c.signup_date) = 2024
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN device IN ('mobile', 'tablet') THEN 'Core' ELSE 'Other' END AS device_group
    FROM joined
    WHERE customer_id IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= s.started_at
        )
),
enriched AS (
    -- Attach customers attributes
    SELECT src.*, p.signup_date
    FROM kept src
    LEFT JOIN customers p
        ON src.customer_id <=> p.id
),
enriched_sessions AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        email,
        ROW_NUMBER() OVER (PARTITION BY MONTH(signup_date), customer_id ORDER BY started_at, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY signup_date DESC, id) AS seq_no_2
    FROM enriched
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.duration_sec
    FROM enriched_sessions s
    JOIN LATERAL (
            SELECT c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(duration_sec) OVER (PARTITION BY customer_id) AS customer_id_avg_duration_sec,
        SUM(seq_no) OVER (PARTITION BY country ORDER BY seq_no_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no
    FROM with_last
)
SELECT
    id,
    device,
    browser,
    email,
    seq_no,
    seq_no_2,
    duration_sec,
    running_seq_no
FROM with_windows
