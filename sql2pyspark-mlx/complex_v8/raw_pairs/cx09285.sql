WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        DATE_TRUNC('QUARTER', read_at) AS read_at_quarter
    FROM meter_readings
    WHERE MONTH(read_at) IN (1, 3)
      AND kwh <= 79.7
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER w_read_at_quarter_to_date_site AS read_at_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY YEAR(read_at_quarter), site_id ORDER BY read_at, id) AS rn,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_quality, 0), 4) AS kwh_share_of_quality,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality), 2) AS kwh_vs_quality_avg,
        LEAD(voltage, 1, 0) OVER (PARTITION BY YEAR(read_at_quarter), site_id ORDER BY read_at, id) AS next_voltage,
        kwh - LAG(kwh) OVER (ORDER BY kwh ASC, id) AS kwh_change
    FROM base
    WINDOW w_read_at_quarter_to_date_site AS (PARTITION BY TO_DATE(read_at_quarter), site_id ORDER BY read_at ASC NULLS LAST, id), w_quality AS (PARTITION BY quality)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(kwh) OVER (PARTITION BY rn, read_at_dense_rank) AS rn_read_at_dense_rank_min_kwh,
        NTH_VALUE(read_at_dense_rank, 2) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at_dense_rank,
        SUM(rn) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rn,
        ROUND(kwh - AVG(kwh) OVER w_rn_read_at_dense_rank, 2) AS kwh_vs_rn_read_at_dense_rank_avg
    FROM ranked
    WINDOW w_rn_read_at_dense_rank AS (PARTITION BY rn, read_at_dense_rank)
),
latest_per_key AS (
    -- Keep the latest row per site_id, quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_slug,
        read_at_quarter,
        read_at_dense_rank,
        rn,
        kwh_share_of_quality,
        kwh_vs_quality_avg,
        next_voltage,
        kwh_change,
        rn_read_at_dense_rank_min_kwh,
        second_read_at_dense_rank,
        running_rn,
        kwh_vs_rn_read_at_dense_rank_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id, quality ORDER BY kwh DESC, id DESC) AS row_num
        FROM with_trends
    ) ranked
    WHERE row_num = 1
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(CASE WHEN quality IS NOT NULL THEN 1 END) AS reading_quality_count,
        COUNT(CASE WHEN quality = 'BAD' THEN 1 END) AS reading_quality_count_2,
        ROUND(AVG(voltage), 2) AS avg_reading_voltage
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        COALESCE(m.reading_quality_count_2, 0) AS reading_quality_count_2,
        m.avg_reading_voltage
    FROM latest_per_key s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
labeled AS (
    -- Label and score rows
    SELECT *, ROUND(kwh_share_of_quality * 1.07, 2) AS kwh_share_of_quality_adj
    FROM summarized_rows
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT read_at_dense_rank, MAX(kwh) AS max_kwh, COUNT(*) AS cnt
    FROM labeled
    GROUP BY read_at_dense_rank
    HAVING COUNT(*) >= 3
)
-- Final projection
SELECT
    read_at_dense_rank,
    max_kwh,
    cnt,
    COALESCE(read_at_dense_rank, 0) AS read_at_dense_rank_filled
FROM grouped
