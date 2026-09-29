WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE voltage > 215.1
      AND site_id IS NOT NULL
),
combined_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM prepared s
    JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        site_commissioned_on,
        ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh ASC, id) AS kwh_seq,
        ROW_NUMBER() OVER (PARTITION BY site_zone ORDER BY kwh NULLS LAST, id) AS seq_no,
        SUM(kwh) OVER w_site_zone_site_id AS site_zone_site_id_sum_kwh,
        MIN(kwh) OVER (PARTITION BY site_zone, site_id) AS site_zone_site_id_min_kwh,
        MAX(kwh) OVER (PARTITION BY site_zone, site_id) AS site_zone_site_id_max_kwh
    FROM combined_details
    WINDOW w_site_zone_site_id AS (PARTITION BY site_zone, site_id)
),
enriched AS (
    -- Derive helper columns
    SELECT *, ROUND(kwh_seq / NULLIF(seq_no, 0), 3) AS kwh_seq_per_seq_no
    FROM with_trends
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        read_at + INTERVAL 15 MINUTES AS read_at_plus_15m,
        read_at + INTERVAL 7 DAYS AS read_at_plus_7d
    FROM enriched
    WHERE site_id IS NOT NULL
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(kwh_seq_per_seq_no / NULLIF(seq_no, 0), 3) AS kwh_seq_per_seq_no_per_seq_no,
        DATE_ADD(site_commissioned_on, 30) AS site_commissioned_on_plus_30d,
        LEAST(kwh_seq, kwh_seq_per_seq_no) AS least_kwh_seq_kwh_seq_per_seq_no
    FROM annotated
    WHERE HOUR(read_at_plus_15m) BETWEEN 8 AND 11
)
SELECT
    id,
    site_id,
    read_at,
    site_commissioned_on,
    kwh_seq,
    seq_no,
    site_zone_site_id_min_kwh,
    kwh_seq_per_seq_no,
    read_at_plus_15m,
    read_at_plus_7d,
    least_kwh_seq_kwh_seq_per_seq_no,
    DATE_FORMAT(read_at_plus_7d, 'yyyy-MM-dd') AS read_at_plus_7d_label,
    read_at_plus_15m + INTERVAL 2 HOURS AS read_at_plus_15m_plus_2h
FROM scored
ORDER BY read_at_plus_15m_plus_2h DESC, read_at NULLS LAST
