WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('BAD', 'OK') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE kwh >= 29.2
      AND site_id <= 7
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        si.commissioned_on AS site_commissioned_on
    FROM prepared s
    JOIN sites si ON si.id = s.site_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY TO_DATE(read_at), site_id), 2) AS kwh_vs_read_at_to_date_site_id_avg,
        ROW_NUMBER() OVER w_site_id_site_commissioned_on AS site_commissioned_on_seq,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), quality ORDER BY read_at) AS read_at_dense_rank,
        MIN(kwh) OVER (PARTITION BY quality) AS quality_min_kwh,
        DENSE_RANK() OVER (PARTITION BY quality ORDER BY read_at ASC) AS read_at_dense_rank_2,
        MAX(kwh) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS kwh_max_7,
        LAG(kwh) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_kwh,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), quality ORDER BY read_at), 3) AS read_at_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY read_at), 3) AS read_at_cume_dist_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality ORDER BY read_at ASC), 3) AS read_at_percent_rank,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY site_commissioned_on DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_7,
        COUNT(*) OVER (PARTITION BY TO_DATE(read_at), site_id) AS rows_in_read_at_to_date_site_id,
        RANK() OVER (PARTITION BY quality ORDER BY read_at ASC) AS read_at_rank,
        LAST_VALUE(kwh) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh
    FROM joined
    WINDOW w_site_id_site_commissioned_on AS (PARTITION BY site_id, DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY site_commissioned_on DESC, id)
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality,
    site_commissioned_on,
    site_commissioned_on_seq,
    quality_min_kwh,
    read_at_dense_rank_2,
    kwh_max_7,
    read_at_cume_dist,
    read_at_percent_rank,
    kwh_avg_7,
    rows_in_read_at_to_date_site_id,
    last_kwh,
    DATE_TRUNC('QUARTER', read_at) AS read_at_quarter
FROM sequenced
