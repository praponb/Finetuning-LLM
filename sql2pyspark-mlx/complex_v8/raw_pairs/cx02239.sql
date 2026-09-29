WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-02-24'
      AND quality <> 'OK'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(quality_group) OVER w_read_at_month_site_id) AS distinct_quality_group_in_read_at_month_site_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY kwh DESC), 3) AS kwh_percent_rank,
        MAX(kwh) OVER (ORDER BY read_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kwh_max_3,
        ROW_NUMBER() OVER (ORDER BY read_at ASC, id) AS seq_no,
        LEAD(kwh, 2) OVER (ORDER BY read_at, id) AS next2_kwh,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY MONTH(read_at), site_id), 2) AS kwh_vs_read_at_month_site_id_avg,
        COUNT(*) OVER w_read_at_month_site_id AS rows_in_read_at_month_site_id
    FROM cleaned_readings
    WINDOW w_read_at_month_site_id AS (PARTITION BY MONTH(read_at), site_id)
),
derived AS (
    -- Label and score rows
    SELECT *, COALESCE(seq_no, 0) AS seq_no_filled
    FROM metrics
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality_group,
        distinct_quality_group_in_read_at_month_site_id,
        kwh_percent_rank,
        seq_no,
        next2_kwh,
        kwh_vs_read_at_month_site_id_avg,
        rows_in_read_at_month_site_id,
        seq_no_filled,
        ROUND(seq_no - AVG(seq_no) OVER (PARTITION BY distinct_quality_group_in_read_at_month_site_id, seq_no_filled), 2) AS seq_no_vs_distinct_quality_group_in_read_at_mont,
        RANK() OVER (ORDER BY kwh_percent_rank DESC) AS kwh_percent_rank_rank,
        ROUND(AVG(kwh) OVER (ORDER BY kwh_percent_rank DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_6_prior,
        COUNT(*) OVER (PARTITION BY distinct_quality_group_in_read_at_month_site_id, seq_no_filled) AS rows_in_distinct_quality_group_in_read_at_month_,
        MAX(kwh) OVER w_distinct_quality_group_in_re AS distinct_quality_group_in_read_at_month_site_id_,
        ROUND(seq_no_filled - AVG(seq_no_filled) OVER (PARTITION BY distinct_quality_group_in_read_at_month_site_id, seq_no_filled), 2) AS seq_no_filled_vs_distinct_quality_group_in_read_
    FROM derived
    WINDOW w_distinct_quality_group_in_re AS (PARTITION BY distinct_quality_group_in_read_at_month_site_id, seq_no_filled)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality_group,
    distinct_quality_group_in_read_at_month_site_id,
    kwh_percent_rank,
    seq_no,
    rows_in_read_at_month_site_id,
    seq_no_filled,
    seq_no_vs_distinct_quality_group_in_read_at_mont,
    kwh_percent_rank_rank,
    kwh_avg_6_prior,
    rows_in_distinct_quality_group_in_read_at_month_,
    LOWER(REGEXP_REPLACE(TRIM(quality_group), ' ', '_')) AS quality_group_slug
FROM with_windows
ORDER BY site_id DESC NULLS FIRST, seq_no_vs_distinct_quality_group_in_read_at_mont
