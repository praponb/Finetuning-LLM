WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(device) device_length,
        COALESCE(country, 'OTHER') resolved_country
    FROM sessions
    WHERE country IS NOT NULL
      AND country = 'TH'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        device,
        country,
        device_length,
        resolved_country
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY device_length DESC, id ASC) = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        device_length,
        resolved_country,
        LOWER(country) country_lower
    FROM latest
    WHERE country IS DISTINCT FROM 'SG'
),
latest_sessions AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY device_length DESC NULLS FIRST, id ASC) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_length,
        resolved_country,
        country_lower,
        AVG(device_length) OVER w_part device_length_avg_2_prior,
        FIRST_VALUE(device_length) IGNORE NULLS OVER (ORDER BY device_length ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_device_length
    FROM latest_sessions
    WINDOW w_part AS (PARTITION BY device ORDER BY device_length ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(device_length_avg_2_prior * 100, 2) adjusted_device_length_avg_2_prior,
        ROUND(first_known_device_length * 100, 2) first_known_device_length_adjusted
    FROM metrics
    WHERE device_length >= 35
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.page = '/product'
        )
)
SELECT
    id,
    device_length,
    country_lower,
    device_length_avg_2_prior,
    first_known_device_length,
    adjusted_device_length_avg_2_prior
FROM eligible;
