WITH enriched AS (
    -- Ranking and gap features
    SELECT *, LAST_VALUE(browser, TRUE) OVER (PARTITION BY country, browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser
    FROM sessions
    WHERE customer_id IS NOT NULL
),
enriched_sessions AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        ROW_NUMBER() OVER (PARTITION BY browser, device ORDER BY started_at, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS event_seq_2
    FROM enriched
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN country IN ('TH', 'UK') THEN 'Core' ELSE 'Other' END AS country_group
    FROM enriched_sessions
    WHERE country IS NOT DISTINCT FROM 'US'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(event_seq_2) OVER w_recent AS running_avg_event_seq_2,
        MAX(event_seq_2) OVER w_recent AS running_max_event_seq_2,
        SUM(event_seq_2) OVER w_recent AS running_sum_event_seq_2
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY customer_id ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
        )
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.browser,
        e.country,
        e.event_seq,
        e.event_seq_2,
        e.running_avg_event_seq_2,
        c.age
    FROM not_flagged e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
enriched_sessions_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(age) OVER (ORDER BY event_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_age,
        AVG(event_seq_2) OVER (PARTITION BY customer_id ORDER BY running_avg_event_seq_2, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS event_seq_2_avg_2_prior,
        LAST_VALUE(age, TRUE) OVER (PARTITION BY customer_id ORDER BY running_avg_event_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_age
    FROM with_dims
)
SELECT
    id,
    browser,
    event_seq_2,
    running_avg_event_seq_2,
    event_seq_2_avg_2_prior
FROM enriched_sessions_2
ORDER BY running_avg_event_seq_2 NULLS LAST, event_seq_2
