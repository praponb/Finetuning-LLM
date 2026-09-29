WITH cleaned_sites AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket
    FROM sites
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        installed_kw_bucket - LAG(installed_kw_bucket) OVER w_all_by_installed_kw_bucket AS installed_kw_bucket_change,
        ROUND(PERCENT_RANK() OVER (ORDER BY installed_kw_bucket DESC, id), 3) AS installed_kw_bucket_percent_rank,
        ROUND(installed_kw_bucket / NULLIF(SUM(installed_kw_bucket) OVER w_zone, 0), 4) AS installed_kw_bucket_share_of_zone
    FROM cleaned_sites
    WINDOW w_all_by_installed_kw_bucket AS (ORDER BY installed_kw_bucket DESC, id), w_zone AS (PARTITION BY zone)
),
deduped AS (
    -- Keep the latest row per zone
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        installed_kw_bucket,
        installed_kw_bucket_change,
        installed_kw_bucket_percent_rank,
        installed_kw_bucket_share_of_zone
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone ORDER BY installed_kw_bucket_change DESC, id DESC) AS row_num
        FROM ranked
    ) ranked
    WHERE row_num = 1
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM deduped s
    WHERE s.id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.kwh > 48.2
              AND m.quality = 'OK'
        )
      AND s.installed_kw_bucket_percent_rank <= 0.8
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(installed_kw_bucket_change) IGNORE NULLS OVER (PARTITION BY zone ORDER BY installed_kw_bucket_percent_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_installed_kw_bucket_change,
        FIRST_VALUE(installed_kw_bucket_change) OVER (PARTITION BY zone ORDER BY installed_kw_bucket_percent_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_installed_kw_bucket_change,
        ROUND(AVG(installed_kw) OVER (PARTITION BY zone ORDER BY installed_kw_bucket_percent_rank ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS installed_kw_avg_7,
        SUM(installed_kw_bucket_change) OVER w_zone AS zone_sum_installed_kw_bucket_change
    FROM filtered
    WINDOW w_zone AS (PARTITION BY zone)
)
-- Final projection
SELECT
    id,
    site_name,
    zone,
    installed_kw,
    installed_kw_bucket_change,
    last_known_installed_kw_bucket_change,
    installed_kw_avg_7,
    zone_sum_installed_kw_bucket_change,
    CONCAT(zone, '#', CAST(installed_kw_bucket AS STRING)) AS zone_ref
FROM with_trends
ORDER BY zone_sum_installed_kw_bucket_change DESC NULLS FIRST, last_known_installed_kw_bucket_change DESC
