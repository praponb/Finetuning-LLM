WITH valid_readings AS (
    -- Filter meter_readings
    SELECT *
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM valid_readings
    WHERE voltage <= 231.7
)
SELECT
    quality,
    site_id,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
FROM scored
GROUP BY GROUPING SETS ((quality, site_id), (quality))
ORDER BY quality NULLS LAST, site_id NULLS LAST
