WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE quality = 'BAD'
),
enriched AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.days_since_read_at,
        si.site_name,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM base s
    LEFT JOIN sites si ON si.id = s.site_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY site_tariff ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        ROW_NUMBER() OVER (PARTITION BY site_tariff ORDER BY read_at, id) AS rn,
        NTILE(4) OVER (ORDER BY site_commissioned_on DESC, id) AS site_commissioned_on_quartile,
        ROUND(days_since_read_at / NULLIF(SUM(days_since_read_at) OVER w_site_tariff_site_commissione, 0), 4) AS days_since_read_at_share_of_site_tariff_site_com,
        LEAD(site_commissioned_on) OVER (ORDER BY site_commissioned_on DESC, id) AS next_site_commissioned_on,
        SUM(days_since_read_at) OVER (PARTITION BY YEAR(site_commissioned_on)) AS site_commissioned_on_year_sum_days_since_read_at,
        DENSE_RANK() OVER w_all_by_site_commissioned_on AS site_commissioned_on_dense_rank,
        MIN(days_since_read_at) OVER (PARTITION BY site_tariff, YEAR(site_commissioned_on)) AS site_tariff_site_commissioned_on_year_min_days_s,
        ROUND(days_since_read_at / NULLIF(SUM(days_since_read_at) OVER w_site_commissioned_on_year, 0), 4) AS days_since_read_at_share_of_site_commissioned_on,
        SUM(days_since_read_at) OVER (ORDER BY days_since_read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_read_at,
        SIZE(COLLECT_SET(site_tariff) OVER (PARTITION BY YEAR(site_commissioned_on))) AS distinct_site_tariff_in_site_commissioned_on_yea
    FROM enriched
    WINDOW w_all_by_site_commissioned_on AS (ORDER BY site_commissioned_on DESC, id), w_site_tariff_site_commissione AS (PARTITION BY site_tariff, YEAR(site_commissioned_on)), w_site_commissioned_on_year AS (PARTITION BY YEAR(site_commissioned_on))
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        GREATEST(days_since_read_at, rn) AS greatest_days_since_read_at_rn,
        GREATEST(rn, days_since_read_at) AS greatest_rn_days_since_read_at
    FROM ranked
),
totals AS (
    -- Group-level totals
    SELECT
        site_tariff,
        site_id,
        rn,
        COUNT(*) AS n_rows,
        SUM(days_since_read_at_share_of_site_tariff_site_com) AS total_days_since_read_at_share_of_site_tariff_si
    FROM labeled
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 3
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        site_tariff,
        site_id,
        rn,
        total_days_since_read_at_share_of_site_tariff_si,
        n_rows - LAG(n_rows) OVER (PARTITION BY site_id, rn ORDER BY total_days_since_read_at_share_of_site_tariff_si DESC, site_tariff, site_id, rn) AS n_rows_change,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id, rn ORDER BY total_days_since_read_at_share_of_site_tariff_si DESC), 3) AS total_days_since_read_at_share_of_site_tariff_si_2
    FROM totals
)
SELECT
    site_tariff,
    rn,
    total_days_since_read_at_share_of_site_tariff_si,
    n_rows_change,
    total_days_since_read_at_share_of_site_tariff_si_2,
    n_rows_change + rn AS n_rows_change_plus_rn,
    LOWER(REGEXP_REPLACE(TRIM(site_tariff), ' ', '_')) AS site_tariff_slug
FROM windowed
ORDER BY n_rows_change, n_rows_change_plus_rn NULLS LAST
