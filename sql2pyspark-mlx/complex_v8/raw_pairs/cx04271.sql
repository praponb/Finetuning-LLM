WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        DATE_ADD(commissioned_on, 30) AS commissioned_on_plus_30d,
        ROUND(installed_kw * 0.25, 2) AS installed_kw_adj,
        CONCAT(site_name, '#', CAST(id AS STRING)) AS site_name_ref
    FROM sites
    WHERE (LOWER(site_name) LIKE '% b%' OR commissioned_on < DATE '2024-11-21')
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM cleaned s
    WHERE s.id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.quality NOT IN ('OK', 'BAD')
        )
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN tariff IN ('TOU', 'EV') THEN 'group_a' WHEN tariff = 'FLAT' THEN 'group_b' ELSE 'other' END AS tariff_group,
        CONCAT_WS('-', zone, tariff) AS zone_tariff_label
    FROM qualified
    WHERE installed_kw_adj >= 154.0
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.voltage) FROM meter_readings m WHERE m.site_id = s.id) AS min_reading_voltage
    FROM annotated s
),
tariff_group_stats AS (
    -- Median installed_kw_adj per tariff_group
    SELECT tariff_group, PERCENTILE_APPROX(installed_kw_adj, 0.5) AS median_installed_kw_adj
    FROM summarized_rows
    GROUP BY tariff_group
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_installed_kw_adj,
        ROUND(s.installed_kw_adj - g.median_installed_kw_adj, 2) AS installed_kw_adj_minus_median
    FROM summarized_rows s
    JOIN tariff_group_stats g ON g.tariff_group = s.tariff_group
    WHERE s.installed_kw_adj >= g.median_installed_kw_adj
),
summary AS (
    -- Group-level totals
    SELECT
        tariff,
        DATE_TRUNC('MONTH', commissioned_on) AS commissioned_on_month,
        COUNT(DISTINCT tariff_group) AS distinct_tariff_groups,
        SUM(CASE WHEN tariff = 'TOU' THEN installed_kw ELSE 0 END) AS installed_kw_tariff_tou,
        COUNT(DISTINCT zone) AS distinct_zones,
        COUNT(*) AS row_count
    FROM above_average
    WHERE site_name_ref IS NOT NULL
    GROUP BY 1, 2
    HAVING SUM(min_reading_voltage) > 215.2
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        tariff,
        commissioned_on_month,
        distinct_tariff_groups,
        installed_kw_tariff_tou,
        row_count,
        DENSE_RANK() OVER (ORDER BY distinct_tariff_groups DESC NULLS FIRST, tariff, commissioned_on_month) AS distinct_tariff_groups_dense_rank,
        NTH_VALUE(installed_kw_tariff_tou, 2) OVER (ORDER BY distinct_zones DESC, tariff, commissioned_on_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_installed_kw_tariff_tou,
        NTILE(4) OVER w_all_by_distinct_zones AS distinct_zones_quartile,
        COUNT(*) OVER (PARTITION BY TO_DATE(commissioned_on_month)) AS rows_in_commissioned_on_month_to_date
    FROM summary
    WINDOW w_all_by_distinct_zones AS (ORDER BY distinct_zones DESC, tariff, commissioned_on_month)
)
-- Native SQL UNPIVOT (wide to long)
SELECT tariff, commissioned_on_month, field, reading
FROM (
    SELECT tariff, commissioned_on_month, second_installed_kw_tariff_tou, installed_kw_tariff_tou
    FROM with_trends
) src
UNPIVOT (
    reading FOR field IN (second_installed_kw_tariff_tou, installed_kw_tariff_tou)
) unp
