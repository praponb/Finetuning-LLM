WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND HOUR(m.read_at) BETWEEN 8 AND 14
        )
      AND s.zone IN ('Z2', 'Z3')
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        commissioned_on,
        LOWER(TRIM(tariff)) AS tariff_clean
    FROM filtered
    WHERE commissioned_on < DATE '2024-11-21'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        site_name,
        tariff,
        commissioned_on,
        tariff_clean,
        LAG(commissioned_on, 2) OVER (PARTITION BY zone ORDER BY commissioned_on, id) AS prev2_commissioned_on,
        DATEDIFF(commissioned_on, LAG(commissioned_on) OVER w_zone_by_commissioned_on) AS days_since_prev_commissioned_on,
        ROW_NUMBER() OVER w_zone_by_commissioned_on AS commissioned_on_seq,
        FIRST_VALUE(commissioned_on) IGNORE NULLS OVER (PARTITION BY zone ORDER BY commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_commissioned_on,
        NTH_VALUE(commissioned_on, 2) OVER (PARTITION BY zone ORDER BY commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_commissioned_on
    FROM enriched
    WINDOW w_zone_by_commissioned_on AS (PARTITION BY zone ORDER BY commissioned_on, id)
)
SELECT
    id,
    site_name,
    commissioned_on,
    tariff_clean,
    prev2_commissioned_on,
    commissioned_on_seq,
    first_known_commissioned_on,
    second_commissioned_on
FROM with_trends
ORDER BY prev2_commissioned_on NULLS LAST, commissioned_on DESC
