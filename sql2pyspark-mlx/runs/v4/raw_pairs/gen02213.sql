WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(country) AS clean_country,
        DATE_FORMAT(started_at, 'yyyy-MM') AS started_at_label,
        COALESCE(country, 'UNKNOWN') AS country_clean
    FROM sessions
    WHERE started_at > TO_DATE('2023-09-13')
      AND browser IS NOT NULL
)
-- Ranking and gap features
SELECT
    *,
    FIRST_VALUE(browser) IGNORE NULLS OVER w_grp AS first_known_browser,
    LAST_VALUE(country) IGNORE NULLS OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
    FIRST_VALUE(browser) IGNORE NULLS OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser_2
FROM valid
WINDOW w_grp AS (PARTITION BY device, browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
