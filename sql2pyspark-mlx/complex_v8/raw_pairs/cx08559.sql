WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage BETWEEN 216.8 AND 234.5
      AND TO_DATE(read_at) < DATE '2025-03-02'
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        si.tariff AS site_tariff
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY quality, site_tariff ORDER BY kwh), 3) AS kwh_cume_dist,
        DENSE_RANK() OVER (ORDER BY kwh, id) AS kwh_dense_rank,
        SIZE(COLLECT_SET(site_id) OVER w_site_tariff_quality) AS distinct_site_id_in_site_tariff_quality
    FROM with_details_site
    WINDOW w_site_tariff_quality AS (PARTITION BY site_tariff, quality)
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        voltage,
        site_tariff,
        kwh_cume_dist,
        kwh_dense_rank,
        distinct_site_id_in_site_tariff_quality,
        SUM(kwh) OVER (PARTITION BY quality, site_tariff ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh,
        ROUND(kwh_cume_dist / NULLIF(SUM(kwh_cume_dist) OVER (PARTITION BY site_id, site_tariff), 0), 4) AS kwh_cume_dist_share_of_site_id_site_tariff
    FROM sequenced
),
derived AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(site_tariff)) AS site_tariff_len
    FROM sequenced_2
    WHERE distinct_site_id_in_site_tariff_quality > 3
),
sequenced_3 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(site_tariff) OVER w_kwh_dense_rank) AS distinct_site_tariff_in_kwh_dense_rank,
        voltage - LAG(voltage) OVER (ORDER BY kwh_cume_dist NULLS LAST, id) AS voltage_change,
        SUM(running_kwh) OVER (ORDER BY kwh_cume_dist NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS running_kwh_sum_7
    FROM derived
    WINDOW w_kwh_dense_rank AS (PARTITION BY kwh_dense_rank)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN kwh_cume_dist >= 0.8 THEN 'A' WHEN kwh_cume_dist >= 0.4 THEN 'B' ELSE 'C' END AS kwh_cume_dist_tier,
        FLOOR(running_kwh / 250) * 250 AS running_kwh_bucket,
        SUBSTRING(TRIM(site_tariff), 1, 2) AS site_tariff_prefix
    FROM sequenced_3
)
SELECT
    id,
    voltage,
    site_tariff,
    kwh_cume_dist,
    distinct_site_id_in_site_tariff_quality,
    running_kwh,
    kwh_cume_dist_share_of_site_id_site_tariff,
    site_tariff_len,
    distinct_site_tariff_in_kwh_dense_rank,
    voltage_change,
    running_kwh_sum_7,
    site_tariff_prefix
FROM scored
ORDER BY kwh_cume_dist_share_of_site_id_site_tariff DESC, running_kwh DESC
