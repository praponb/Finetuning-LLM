WITH cleaned AS (
    SELECT
        *,
        UPPER(browser) AS browser_upper,
        COALESCE(country, 'OTHER') AS resolved_country
    FROM sessions
    WHERE customer_id IS NOT NULL
      AND device = 'tablet'
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY country, device ORDER BY started_at, id) AS event_seq
    FROM cleaned
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY seq_no DESC) AS rank_in_group
        FROM enriched
    ) t
    WHERE rank_in_group = 1
),
filtered AS (
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        resolved_country,
        seq_no,
        row_seq,
        rank_in_group,
        CASE WHEN device IN ('mobile', 'desktop') THEN 'Group A' ELSE 'Other' END AS device_group,
        INITCAP(TRIM(browser)) AS browser_title
    FROM deduped
    WHERE started_at >= TO_TIMESTAMP('2024-03-19 14:00:00')
      AND customer_id IS NOT NULL
),
not_flagged AS (
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
        )
)
SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    MAX(started_at) AS last_started_at,
    SUM(CASE WHEN browser = 'Safari' THEN seq_no ELSE 0 END) AS safari_seq_no
FROM not_flagged
GROUP BY 1
ORDER BY safari_seq_no
