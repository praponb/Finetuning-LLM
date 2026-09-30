WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        CONCAT(country, ' - ', device) country_device,
        COALESCE(country, 'NONE') resolved_country
    FROM sessions
    WHERE country IS NOT NULL
      AND started_at >= TO_TIMESTAMP('2023-07-02 18:00:00')
)
-- Keep one row per key (latest first)
SELECT
    id,
    customer_id,
    device,
    started_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at, id) AS latest_rank
    FROM valid
) ranked
WHERE latest_rank = 1
