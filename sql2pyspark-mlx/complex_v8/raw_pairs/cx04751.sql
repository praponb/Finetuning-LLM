WITH scoped_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        DATEDIFF(DATE '2026-01-01', commissioned_on) AS days_since_commissioned_on,
        DAYOFWEEK(commissioned_on) AS commissioned_on_dayofweek
    FROM sites
    WHERE tariff IN ('TOU', 'EV')
      AND installed_kw < 386.6
),
stats AS (
    -- Group-level totals
    SELECT
        tariff,
        commissioned_on_dayofweek,
        SUM(days_since_commissioned_on) AS total_days_since_commissioned_on,
        ROUND(SUM(commissioned_on_dayofweek) / NULLIF(SUM(installed_kw), 0), 3) AS commissioned_on_dayofweek_to_installed_kw_ratio,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT zone) AS distinct_zones
    FROM scoped_sites
    GROUP BY tariff, commissioned_on_dayofweek
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY commissioned_on_dayofweek, n_rows ORDER BY commissioned_on_dayofweek_to_installed_kw_ratio DESC) AS commissioned_on_dayofweek_to_installed_kw_ratio_,
        LEAD(distinct_zones) OVER w_commissioned_on_dayofweek_n_2 AS next_distinct_zones
    FROM stats
    WINDOW w_commissioned_on_dayofweek_n_2 AS (PARTITION BY commissioned_on_dayofweek, n_rows ORDER BY commissioned_on_dayofweek_to_installed_kw_ratio DESC, tariff, commissioned_on_dayofweek)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(tariff)) AS tariff_len,
        GREATEST(n_rows, distinct_zones) AS greatest_n_rows_distinct_zones
    FROM sequenced
)
-- Final projection
SELECT
    tariff,
    commissioned_on_dayofweek_to_installed_kw_ratio,
    distinct_zones,
    commissioned_on_dayofweek_to_installed_kw_ratio_,
    next_distinct_zones,
    tariff_len,
    greatest_n_rows_distinct_zones,
    GREATEST(next_distinct_zones, tariff_len) AS greatest_next_distinct_zones_tariff_len,
    COALESCE(next_distinct_zones, 0) AS next_distinct_zones_filled
FROM derived
ORDER BY commissioned_on_dayofweek_to_installed_kw_ratio DESC NULLS FIRST, next_distinct_zones_filled DESC
