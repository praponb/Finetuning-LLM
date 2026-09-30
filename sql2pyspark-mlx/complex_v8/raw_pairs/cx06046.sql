WITH scoped AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        ABS(installed_kw - 141.6) AS installed_kw_distance,
        LENGTH(TRIM(tariff)) AS tariff_len
    FROM sites
    WHERE site_name IS NOT NULL
),
zone_stats AS (
    -- Median tariff_len per zone
    SELECT zone, PERCENTILE_APPROX(tariff_len, 0.5) AS median_tariff_len
    FROM scoped
    GROUP BY zone
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_tariff_len,
        ROUND(s.tariff_len - g.median_tariff_len, 2) AS tariff_len_minus_median
    FROM scoped s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.tariff_len >= g.median_tariff_len
),
latest AS (
    -- Keep the earliest row per zone
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        installed_kw_distance,
        tariff_len,
        median_tariff_len,
        tariff_len_minus_median
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone ORDER BY tariff_len, id) AS row_num
        FROM compared
    ) ranked
    WHERE row_num = 1
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(installed_kw_distance * 0.9, 2) AS installed_kw_distance_adj,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket
    FROM latest
    WHERE installed_kw_distance < 450.0
)
SELECT
    id,
    site_name,
    zone,
    installed_kw,
    commissioned_on,
    installed_kw_distance,
    median_tariff_len,
    tariff_len_minus_median,
    installed_kw_distance_adj,
    FLOOR(installed_kw_bucket / 100) * 100 AS installed_kw_bucket_bucket
FROM tagged
WHERE median_tariff_len BETWEEN 5.6 AND 10.4
