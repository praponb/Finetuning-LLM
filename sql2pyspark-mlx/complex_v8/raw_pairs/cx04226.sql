WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE voltage BETWEEN 221.8 AND 240.2
      AND quality = 'EST'
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_or_unknown,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on
    FROM filtered s
    JOIN sites si ON si.id = s.site_id AND si.installed_kw > 96.9
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        quality,
        quality_or_unknown,
        site_tariff,
        site_installed_kw,
        site_commissioned_on,
        ROW_NUMBER() OVER w_site_tariff_quality_or_unkno AS rn,
        ROUND(site_installed_kw - AVG(site_installed_kw) OVER (PARTITION BY quality), 2) AS site_installed_kw_vs_quality_avg,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER w_quality, 0), 4) AS site_installed_kw_share_of_quality,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY site_tariff, quality_or_unknown ORDER BY site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality), 0), 4) AS voltage_share_of_quality,
        ROW_NUMBER() OVER w_site_id_quality_or_unknown_b AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY quality, quality_or_unknown ORDER BY kwh, id) AS kwh_seq,
        FIRST_VALUE(site_installed_kw) OVER (PARTITION BY site_id ORDER BY site_installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_site_installed_kw,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality
    FROM with_parent_site
    WINDOW w_site_tariff_quality_or_unkno AS (PARTITION BY site_tariff, quality_or_unknown ORDER BY site_commissioned_on, id), w_quality AS (PARTITION BY quality), w_site_id_quality_or_unknown_b AS (PARTITION BY site_id, quality_or_unknown ORDER BY voltage DESC, id)
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY seq_no, quality) AS rows_in_seq_no_quality,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY rn, TO_DATE(read_at) ORDER BY seq_no ASC, id))) / 60, 1) AS minutes_since_prev_read_at,
        LEAD(rn, 1, 0) OVER (PARTITION BY rn, TO_DATE(read_at) ORDER BY seq_no ASC, id) AS next_rn,
        MIN(voltage_share_of_quality) OVER w_seq_no_quality AS seq_no_quality_min_voltage_share_of_quality,
        DENSE_RANK() OVER (PARTITION BY site_tariff ORDER BY site_installed_kw_share_of_quality DESC) AS site_installed_kw_share_of_quality_dense_rank,
        MAX(site_installed_kw) OVER (PARTITION BY site_tariff ORDER BY site_installed_kw_share_of_quality DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_site_installed_kw_so_far,
        LEAD(rn, 2) OVER (PARTITION BY site_tariff ORDER BY site_installed_kw_share_of_quality DESC, id) AS next2_rn
    FROM sequenced
    WINDOW w_seq_no_quality AS (PARTITION BY seq_no, quality)
)
SELECT
    id,
    site_id,
    read_at,
    quality_or_unknown,
    site_tariff,
    site_commissioned_on,
    site_installed_kw_vs_quality_avg,
    site_installed_kw_share_of_quality,
    second_voltage,
    voltage_share_of_quality,
    seq_no,
    kwh_seq,
    rows_in_quality,
    rows_in_seq_no_quality,
    minutes_since_prev_read_at,
    next_rn,
    site_installed_kw_share_of_quality_dense_rank,
    max_site_installed_kw_so_far,
    next2_rn,
    LENGTH(TRIM(quality_or_unknown)) AS quality_or_unknown_len
FROM sequenced_2
ORDER BY voltage_share_of_quality DESC
