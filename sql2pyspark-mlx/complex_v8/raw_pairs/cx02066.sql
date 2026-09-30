WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE kwh >= 28.4
),
stats AS (
    -- Group-level totals
    SELECT
        site_id,
        quality,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        ROUND(SUM(voltage) / NULLIF(SUM(kwh), 0), 3) AS voltage_to_kwh_ratio,
        MIN(voltage) AS min_voltage
    FROM filtered_readings
    GROUP BY site_id, quality
),
with_parent_site AS (
    -- Join sites
    SELECT s.*, si.commissioned_on AS site_commissioned_on, si.site_name
    FROM stats s
    JOIN sites si ON si.id = s.site_id
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.site_id,
        s.quality,
        s.quality_list,
        s.site_commissioned_on,
        s.site_name,
        si.commissioned_on AS site_commissioned_on_2,
        si.zone AS site_zone
    FROM with_parent_site s
    LEFT JOIN sites si ON si.id = s.site_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        site_id,
        quality,
        quality_list,
        site_name,
        site_commissioned_on_2,
        site_zone,
        RANK() OVER (PARTITION BY site_zone ORDER BY site_commissioned_on DESC) AS site_commissioned_on_rank,
        RANK() OVER (PARTITION BY quality, site_id ORDER BY site_commissioned_on) AS site_commissioned_on_rank_2,
        LAST_VALUE(site_commissioned_on_2) OVER (ORDER BY site_commissioned_on_2 DESC, site_id, quality ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_site_commissioned_on_2,
        DATEDIFF(site_commissioned_on, LAG(site_commissioned_on) OVER (PARTITION BY quality, site_id ORDER BY site_commissioned_on, site_id, quality)) AS days_since_prev_site_commissioned_on
    FROM joined
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(site_commissioned_on_2, 'MMM yyyy') AS site_commissioned_on_2_label,
        ADD_MONTHS(site_commissioned_on_2, 3) AS site_commissioned_on_2_plus_3m
    FROM screened
)
SELECT *
FROM (
    SELECT site_commissioned_on_2_plus_3m, site_commissioned_on_rank_2, site_zone, site_commissioned_on_rank
    FROM enriched
) src
PIVOT (
    COUNT(site_commissioned_on_rank) AS count_v, SUM(site_commissioned_on_rank) AS sum_v
    FOR site_zone IN ('Z4', 'Z3')
)
ORDER BY site_commissioned_on_2_plus_3m, site_commissioned_on_rank_2
