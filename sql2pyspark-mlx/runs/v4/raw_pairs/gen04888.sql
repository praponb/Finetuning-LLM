WITH valid AS (
    SELECT
        *,
        SUBSTRING(browser, 1, 2) AS browser_prefix,
        COALESCE(browser, 'N/A') AS browser_filled
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-12-02 10:00:00'
      AND customer_id IS NOT NULL
),
remaining_keys AS (
    SELECT customer_id
    FROM valid
    INTERSECT ALL
    SELECT customer_id
    FROM sessions
    WHERE customer_id IN (22, 23, 34, 39)
),
kept_rows AS (
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.customer_id = k.customer_id
),
with_dims AS (
    SELECT e.*, p.country AS customer_country
    FROM kept_rows e
    JOIN customers p
        ON e.customer_id = p.id
),
kept AS (
    SELECT s.*
    FROM with_dims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time < s.started_at
        )
),
matched_keys AS (
    SELECT id
    FROM kept
    EXCEPT ALL
    SELECT id
    FROM sessions
    WHERE customer_id IS NULL
),
reconciled AS (
    SELECT s.*
    FROM kept s
    JOIN matched_keys k ON s.id = k.id
),
enriched AS (
    SELECT e.*, d.name
    FROM reconciled e
    LEFT JOIN customers d
        ON e.customer_id = d.id
)
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes,
    COUNT(*) FILTER (WHERE country = 'SG') AS sg_rows,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SIZE(COLLECT_SET(country)) AS num_countries_2,
    GROUPING_ID() AS grouping_id
FROM enriched
GROUP BY ROLLUP (DATE_TRUNC('YEAR', started_at))
HAVING num_countries >= 1
