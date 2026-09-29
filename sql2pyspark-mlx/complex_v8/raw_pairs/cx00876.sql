WITH valid AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        commissioned_on
    FROM sites
),
zone_stats AS (
    -- Avg installed_kw per zone
    SELECT zone, AVG(installed_kw) AS avg_installed_kw
    FROM valid
    GROUP BY zone
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_installed_kw,
        ROUND(s.installed_kw - g.avg_installed_kw, 2) AS installed_kw_minus_avg
    FROM valid s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.installed_kw > g.avg_installed_kw
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM compared s
    WHERE s.id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.quality IN ('EST', 'OK')
              AND m.voltage BETWEEN 216.5 AND 233.1
        )
),
unique_rows AS (
    -- Keep the latest row per zone
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        commissioned_on,
        avg_installed_kw,
        installed_kw_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone ORDER BY installed_kw DESC, id DESC) AS row_num
        FROM without_matches
    ) ranked
    WHERE row_num = 1
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        LAST_DAY(commissioned_on) AS commissioned_on_month_end,
        CONCAT(site_name, '#', CAST(id AS STRING)) AS site_name_ref
    FROM unique_rows
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        commissioned_on,
        avg_installed_kw,
        installed_kw_minus_avg,
        commissioned_on_month_end,
        site_name_ref,
        COUNT(*) OVER w_zone AS rows_in_zone,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', commissioned_on_month_end), zone ORDER BY installed_kw_minus_avg ASC), 3) AS installed_kw_minus_avg_percent_rank,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', commissioned_on_month_end), zone ORDER BY installed_kw_minus_avg ASC, id) AS installed_kw_minus_avg_decile
    FROM scored
    WINDOW w_zone AS (PARTITION BY zone)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(commissioned_on, 'yyyy-MM-dd') AS commissioned_on_label,
        LOWER(REGEXP_REPLACE(TRIM(site_name), ' ', '_')) AS site_name_slug,
        CASE WHEN avg_installed_kw >= 376.2 THEN 'large' WHEN avg_installed_kw >= 178.2 THEN 'medium' ELSE 'small' END AS avg_installed_kw_tier
    FROM with_trends
)
-- Final projection
SELECT
    id,
    site_name,
    zone,
    installed_kw,
    commissioned_on,
    avg_installed_kw,
    installed_kw_minus_avg,
    site_name_ref,
    rows_in_zone,
    commissioned_on_label,
    site_name_slug,
    avg_installed_kw_tier,
    CONCAT_WS('|', commissioned_on_label, site_name_slug) AS commissioned_on_label_site_name_slug_label,
    GREATEST(rows_in_zone, installed_kw_minus_avg) AS greatest_rows_in_zone_installed_kw_minus_avg
FROM annotated
WHERE avg_installed_kw_tier = 'medium'
ORDER BY commissioned_on_label_site_name_slug_label DESC NULLS FIRST, avg_installed_kw_tier
