WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        DATE_TRUNC('HOUR', read_at) AS read_at_hour
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MAX(read_at) AS last_reading_read_at,
        MAX(read_at) AS last_reading_read_at_2,
        COUNT(CASE WHEN quality = 'BAD' THEN 1 END) AS reading_quality_count
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at,
        m.last_reading_read_at_2,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count
    FROM filtered s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
quality_stats AS (
    -- Max reading_quality_count per quality
    SELECT quality, MAX(reading_quality_count) AS max_reading_quality_count
    FROM with_stats
    GROUP BY quality
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_reading_quality_count,
        ROUND(s.reading_quality_count - g.max_reading_quality_count, 2) AS reading_quality_count_minus_max
    FROM with_stats s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.reading_quality_count = g.max_reading_quality_count
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        s.read_at_hour,
        s.last_reading_read_at,
        s.last_reading_read_at_2,
        s.reading_quality_count,
        s.max_reading_quality_count,
        si.zone AS site_zone,
        si.site_name
    FROM vs_group s
    JOIN sites si ON si.id = s.site_id
),
enriched_site AS (
    -- Join sites
    SELECT s.*, si.tariff AS site_tariff
    FROM with_details_site s
    LEFT JOIN sites si ON si.id = s.site_id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        site_id,
        site_zone,
        DATE_TRUNC('MONTH', last_reading_read_at_2) AS last_reading_read_at_2_month,
        ARRAY_SORT(COLLECT_SET(site_tariff)) AS site_tariff_list,
        COUNT(*) AS n_rows,
        MIN(last_reading_read_at_2) AS first_last_reading_read_at_2,
        COUNT(DISTINCT quality) AS distinct_qualitys
    FROM enriched_site
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
exploded AS (
    -- One row per site_tariff_list_item
    SELECT
        s.site_id,
        s.site_zone,
        s.last_reading_read_at_2_month,
        s.n_rows,
        s.first_last_reading_read_at_2,
        s.distinct_qualitys,
        t.site_tariff_list_item_pos,
        t.site_tariff_list_item
    FROM totals s
    LATERAL VIEW POSEXPLODE(s.site_tariff_list) t AS site_tariff_list_item_pos, site_tariff_list_item
)
SELECT
    site_id,
    site_zone,
    last_reading_read_at_2_month,
    n_rows,
    first_last_reading_read_at_2,
    distinct_qualitys,
    site_tariff_list_item_pos,
    site_tariff_list_item
FROM exploded
WHERE site_id IS NOT NULL
ORDER BY site_tariff_list_item
