WITH flattened AS (
    SELECT
        id,
        service,
        level,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, value
),
with_windows AS (
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY service), 4) AS status_code_share,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY service), 4) AS status_code_share_2,
        RANK() OVER (ORDER BY status_code DESC) AS status_code_rank
    FROM flattened
),
with_windows_app_logs AS (
    SELECT
        *,
        STDDEV_SAMP(status_code_share) OVER (PARTITION BY key) AS key_stddev_samp_status_code_share,
        MIN(status_code_share) OVER (PARTITION BY key) AS key_min_status_code_share,
        CUME_DIST() OVER (PARTITION BY level, key ORDER BY status_code) AS status_code_cume_dist,
        CUME_DIST() OVER (PARTITION BY key ORDER BY status_code_share_2) AS status_code_share_2_cume_dist,
        RANK() OVER (PARTITION BY level ORDER BY status_code_share_2 DESC NULLS LAST) AS status_code_share_2_rank
    FROM with_windows
),
scored AS (
    SELECT
        id,
        service,
        level,
        key,
        status_code_share_2,
        status_code_rank,
        status_code_share_2_cume_dist,
        status_code_share_2_rank,
        RANK() OVER (PARTITION BY key ORDER BY status_code_share_2 DESC NULLS LAST) AS status_code_share_2_rank_2,
        COUNT(status_code_share) OVER w_ordered AS level_count_status_code_share,
        MAX(status_code_share_2_cume_dist) OVER w_ordered AS level_max_status_code_share_2_cume_dist
    FROM with_windows_app_logs
    WHERE status_code_share <= 0.45
    WINDOW w_ordered AS (PARTITION BY level)
),
filtered AS (
    SELECT *, LEAST(status_code_share_2_rank, status_code_rank) AS least_status_code_share_2_rank_status_code_rank
    FROM scored
    WHERE key IS NOT NULL
),
daily AS (
    SELECT
        CASE WHEN least_status_code_share_2_rank_status_code_rank >= 74 THEN 'LARGE' ELSE 'SMALL' END AS least_status_code_share_2_rank_status_code_rank_tier,
        service,
        CASE WHEN status_code_share_2_cume_dist >= 0.6 THEN 'LARGE' ELSE 'SMALL' END AS status_code_share_2_cume_dist_tier,
        ROUND(SUM(status_code_share_2_rank) / NULLIF(COUNT(CASE WHEN key = 'version' THEN 1 END), 0), 2) AS status_code_share_2_rank_per_version,
        COUNT(CASE WHEN key = 'region' THEN 1 END) AS region_count,
        MAX(status_code_share_2) AS max_status_code_share_2
    FROM filtered
    GROUP BY CASE WHEN least_status_code_share_2_rank_status_code_rank >= 74 THEN 'LARGE' ELSE 'SMALL' END, service, CASE WHEN status_code_share_2_cume_dist >= 0.6 THEN 'LARGE' ELSE 'SMALL' END
    HAVING status_code_share_2_rank_per_version > 0
)
SELECT
    least_status_code_share_2_rank_status_code_rank_tier,
    service,
    status_code_share_2_cume_dist_tier,
    status_code_share_2_rank_per_version,
    region_count,
    max_status_code_share_2
FROM daily
ORDER BY max_status_code_share_2 DESC
