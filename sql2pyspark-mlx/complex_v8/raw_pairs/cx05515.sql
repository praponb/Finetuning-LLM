WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CASE WHEN quality IN ('BAD', 'OK') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 7 AND 11
      AND site_id <= 4
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(kwh) OVER (PARTITION BY MONTH(read_at) ORDER BY read_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS kwh_sum_6_prior,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at_month), quality_group)) AS distinct_quality_in_read_at_month_month_quality_,
        SUM(kwh) OVER (PARTITION BY MONTH(read_at) ORDER BY read_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS kwh_sum_2_prior,
        ROW_NUMBER() OVER (PARTITION BY MONTH(read_at) ORDER BY read_at DESC, id) AS read_at_seq
    FROM recent
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM metrics
    WHERE kwh_sum_6_prior > (SELECT MIN(kwh_sum_6_prior) FROM metrics)
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM filtered s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.voltage >= 215.7
              AND HOUR(m.read_at) BETWEEN 7 AND 12
        )
      AND s.kwh BETWEEN 9.5 AND 117.4
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        kwh_sum_6_prior + read_at_seq AS kwh_sum_6_prior_plus_read_at_seq,
        ROUND(read_at_seq / NULLIF(kwh, 0), 3) AS read_at_seq_per_kwh
    FROM with_activity
    WHERE read_at >= TIMESTAMP '2025-01-25 11:00:00'
),
combined_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.quality_group,
        s.read_at_month,
        s.read_at_label,
        s.kwh_sum_2_prior,
        s.read_at_seq_per_kwh,
        si.zone AS site_zone
    FROM annotated s
    JOIN sites si ON si.id = s.site_id
),
quality_group_stats AS (
    -- Median read_at_seq_per_kwh per quality_group
    SELECT quality_group, PERCENTILE_APPROX(read_at_seq_per_kwh, 0.5) AS median_read_at_seq_per_kwh
    FROM combined_details
    GROUP BY quality_group
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_read_at_seq_per_kwh,
        ROUND(s.read_at_seq_per_kwh - g.median_read_at_seq_per_kwh, 2) AS read_at_seq_per_kwh_minus_median
    FROM combined_details s
    JOIN quality_group_stats g ON g.quality_group = s.quality_group
    WHERE s.read_at_seq_per_kwh >= g.median_read_at_seq_per_kwh
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    quality_group,
    read_at_month,
    kwh_sum_2_prior,
    read_at_seq_per_kwh,
    site_zone,
    median_read_at_seq_per_kwh
FROM compared
WHERE HOUR(read_at) BETWEEN 10 AND 14
