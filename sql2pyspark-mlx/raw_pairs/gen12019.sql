WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        HOUR(started_at) AS started_at_hour,
        COALESCE(country, 'UNKNOWN') AS country_clean
    FROM sessions
    WHERE country IS NOT DISTINCT FROM 'TH'
      AND customer_id IN (3, 30)
)
-- QUALIFY-style window filter
SELECT
    id,
    customer_id,
    started_at_hour,
    country_clean
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at_hour NULLS FIRST, id) = 1;
