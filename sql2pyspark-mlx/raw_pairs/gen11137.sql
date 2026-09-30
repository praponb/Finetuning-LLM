WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        DATE_FORMAT(started_at, 'MMM yyyy') AS started_at_label,
        COALESCE(country, 'DIRECT') AS country_filled
    FROM sessions
    WHERE browser IS NOT NULL
      AND TO_DATE(started_at) >= '2023-04-01'
)
-- Ranking and gap features
SELECT
    *,
    LAST_VALUE(browser) IGNORE NULLS OVER w_part AS last_known_browser,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS event_seq,
    ROW_NUMBER() OVER w_grp AS event_seq_2,
    ROW_NUMBER() OVER w_ordered AS seq_no
FROM valid
WINDOW w_part AS (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY started_at, id), w_ordered AS (PARTITION BY browser, country ORDER BY started_at, id);
