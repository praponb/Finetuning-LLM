WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 1.1, 2) AS kwh_adj,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
    FROM meter_readings
    WHERE site_id >= 2
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.kwh_adj,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM valid s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        kwh_adj,
        reading_id,
        reading_quality,
        reading_kwh,
        DATE_FORMAT(reading_read_at, 'MMM yyyy') AS reading_read_at_label,
        CASE WHEN reading_quality IN ('BAD', 'EST') THEN 'group_a' WHEN reading_quality = 'OK' THEN 'group_b' ELSE 'other' END AS reading_quality_group
    FROM exploded_detail_readings
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(reading_kwh) OVER (PARTITION BY reading_quality_group, reading_id) AS reading_quality_group_reading_id_min_reading_kwh,
        ROUND(PERCENT_RANK() OVER (ORDER BY kwh_adj DESC), 3) AS kwh_adj_percent_rank,
        LEAD(kwh_adj, 3) OVER w_all_by_kwh_adj AS next3_kwh_adj,
        ROUND(kwh_adj - AVG(kwh_adj) OVER w_reading_quality_group_readin, 2) AS kwh_adj_vs_reading_quality_group_reading_id_avg,
        COUNT(*) OVER w_reading_quality_group_readin AS rows_in_reading_quality_group_reading_id
    FROM scored
    WINDOW w_all_by_kwh_adj AS (ORDER BY kwh_adj DESC, id, reading_id), w_reading_quality_group_readin AS (PARTITION BY reading_quality_group, reading_id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        kwh_adj,
        reading_id,
        reading_quality,
        reading_read_at_label,
        kwh_adj_percent_rank,
        next3_kwh_adj,
        kwh_adj_vs_reading_quality_group_reading_id_avg,
        ROUND(AVG(kwh_adj) OVER (PARTITION BY reading_quality_group), 2) AS reading_quality_group_avg_kwh_adj,
        DENSE_RANK() OVER (PARTITION BY reading_quality_group, reading_id ORDER BY next3_kwh_adj, id, reading_id) AS next3_kwh_adj_dense_rank,
        ROUND(kwh_adj / NULLIF(SUM(kwh_adj) OVER (PARTITION BY reading_quality_group), 0), 4) AS kwh_adj_share_of_reading_quality_group,
        COUNT(*) OVER (PARTITION BY reading_quality_group) AS rows_in_reading_quality_group
    FROM metrics
)
SELECT *
FROM (
    SELECT reading_id, reading_quality, rows_in_reading_quality_group
    FROM ranked
) src
PIVOT (
    SUM(rows_in_reading_quality_group)
    FOR reading_quality IN ('BAD', 'EST', 'OK')
)
