WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.kwh) AS prior_kwh_1d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.prior_rows_1d,
        s.prior_kwh_1d,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM covered s
    LEFT JOIN sites si ON si.id = s.site_id AND si.site_name IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        prior_rows_1d,
        site_zone,
        site_commissioned_on,
        site_tariff,
        ROW_NUMBER() OVER (ORDER BY prior_rows_1d DESC NULLS FIRST, id) AS rn,
        NTH_VALUE(prior_kwh_1d, 2) OVER (ORDER BY prior_rows_1d DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prior_kwh_1d,
        prior_rows_1d - LAG(prior_rows_1d) OVER (ORDER BY prior_rows_1d DESC NULLS FIRST, id) AS prior_rows_1d_change,
        SUM(prior_rows_1d) OVER (ORDER BY prior_rows_1d DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS prior_rows_1d_sum_7,
        ROUND(prior_kwh_1d - AVG(prior_kwh_1d) OVER (PARTITION BY site_id, site_zone), 2) AS prior_kwh_1d_vs_site_id_site_zone_avg
    FROM joined_site
),
site_zone_stats AS (
    -- Median prior_rows_1d per site_zone
    SELECT site_zone, PERCENTILE_APPROX(prior_rows_1d, 0.5) AS median_prior_rows_1d
    FROM ranked
    GROUP BY site_zone
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_prior_rows_1d,
        ROUND(s.prior_rows_1d - g.median_prior_rows_1d, 2) AS prior_rows_1d_minus_median
    FROM ranked s
    JOIN site_zone_stats g ON g.site_zone = s.site_zone
    WHERE s.prior_rows_1d >= g.median_prior_rows_1d
)
SELECT
    id,
    prior_rows_1d,
    site_zone,
    site_commissioned_on,
    rn,
    second_prior_kwh_1d,
    prior_rows_1d_change,
    prior_kwh_1d_vs_site_id_site_zone_avg,
    ROUND(prior_rows_1d / NULLIF(prior_rows_1d_sum_7, 0), 3) AS prior_rows_1d_per_prior_rows_1d_sum_7,
    rn + prior_rows_1d AS rn_plus_prior_rows_1d
FROM compared
ORDER BY prior_kwh_1d_vs_site_id_site_zone_avg DESC, site_commissioned_on, id
LIMIT 5
