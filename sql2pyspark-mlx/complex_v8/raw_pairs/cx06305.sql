WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label
    FROM meter_readings
    WHERE kwh >= 35.9
      AND voltage <= 232.7
),
with_parent AS (
    -- Join sites
    SELECT s.*, si.installed_kw AS site_installed_kw
    FROM valid_readings s
    JOIN sites si ON si.id = s.site_id AND si.site_name IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY quality, site_id) AS rows_in_quality_site_id,
        LAG(quality_len, 3) OVER (PARTITION BY site_id ORDER BY voltage ASC, id) AS prev3_quality_len,
        RANK() OVER w_all_by_quality_len_ties AS quality_len_rank,
        ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY read_at NULLS LAST, id) AS read_at_seq,
        ROW_NUMBER() OVER (PARTITION BY quality ORDER BY read_at DESC, id) AS seq_no,
        COUNT(*) OVER w_read_at_to_date AS rows_in_read_at_to_date,
        NTH_VALUE(quality_len, 2) OVER (PARTITION BY site_id ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_quality_len,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY site_id ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        DENSE_RANK() OVER (PARTITION BY quality, quality_len ORDER BY site_installed_kw) AS site_installed_kw_dense_rank,
        MIN(kwh) OVER (PARTITION BY quality, site_id) AS quality_site_id_min_kwh,
        LAG(kwh) OVER (PARTITION BY quality ORDER BY read_at DESC, id) AS prev_kwh
    FROM with_parent
    WINDOW w_all_by_quality_len_ties AS (ORDER BY quality_len DESC), w_read_at_to_date AS (PARTITION BY TO_DATE(read_at))
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_len,
        read_at_label,
        site_installed_kw,
        rows_in_quality_site_id,
        prev3_quality_len,
        quality_len_rank,
        read_at_seq,
        rows_in_read_at_to_date,
        second_quality_len,
        second_kwh,
        site_installed_kw_dense_rank,
        quality_site_id_min_kwh,
        prev_kwh
    FROM with_trends
    WHERE seq_no <= 5
      AND quality_len_rank < 8
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_len,
        read_at_label,
        prev3_quality_len,
        quality_len_rank,
        rows_in_read_at_to_date,
        second_quality_len,
        second_kwh,
        site_installed_kw_dense_rank,
        quality_site_id_min_kwh,
        prev_kwh,
        ROUND(AVG(quality_site_id_min_kwh) OVER (PARTITION BY quality_len_rank ORDER BY prev_kwh DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS quality_site_id_min_kwh_avg_3_prior,
        LEAD(quality_len, 1, 0) OVER (PARTITION BY quality_len_rank ORDER BY prev_kwh DESC NULLS FIRST, id) AS next_quality_len,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id, quality_len_rank), 2) AS voltage_vs_site_id_quality_len_rank_avg,
        ROUND(read_at_seq - AVG(read_at_seq) OVER (PARTITION BY quality), 2) AS read_at_seq_vs_quality_avg
    FROM top_rows
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality_len,
    rows_in_read_at_to_date,
    second_quality_len,
    second_kwh,
    site_installed_kw_dense_rank,
    prev_kwh,
    next_quality_len,
    voltage_vs_site_id_quality_len_rank_avg
FROM ranked
ORDER BY voltage, voltage_vs_site_id_quality_len_rank_avg NULLS LAST, id
LIMIT 10
