WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, v
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        host,
        level,
        k,
        v,
        LENGTH(host) AS host_length,
        UPPER(service) AS service_upper,
        COALESCE(k, 'N/A') AS k_clean
    FROM exploded
    WHERE k IS NOT NULL
      AND service <> 'billing'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        host,
        level,
        k,
        v,
        CUME_DIST() OVER (PARTITION BY level ORDER BY host_length DESC NULLS LAST) AS host_length_cume_dist,
        RANK() OVER (PARTITION BY k ORDER BY host_length DESC NULLS FIRST) AS host_length_rank,
        RANK() OVER (PARTITION BY host ORDER BY host_length DESC) AS host_length_rank_2,
        PERCENT_RANK() OVER (ORDER BY host_length DESC NULLS FIRST) AS host_length_percent_rank,
        STDDEV_SAMP(host_length) OVER (PARTITION BY service) AS service_stddev_samp_host_length
    FROM cleaned
),
base AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(host) AS host_lower
    FROM with_windows
    WHERE host_length_cume_dist <= 0.42
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        host,
        k,
        v,
        host_length_cume_dist,
        host_length_rank,
        host_length_rank_2,
        ROUND(host_length_rank / SUM(host_length_rank) OVER (PARTITION BY level), 4) AS host_length_rank_share,
        COUNT(host_length_cume_dist) OVER (PARTITION BY host, level) AS host_count_host_length_cume_dist,
        AVG(host_length_rank) OVER (PARTITION BY level) AS level_avg_host_length_rank,
        CUME_DIST() OVER (PARTITION BY host ORDER BY host_length_percent_rank ASC NULLS FIRST) AS host_length_percent_rank_cume_dist
    FROM base
    WHERE k = 'trace'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        host,
        k,
        v,
        host_length_cume_dist,
        host_length_rank,
        host_length_rank_2,
        host_length_rank_share,
        host_count_host_length_cume_dist,
        level_avg_host_length_rank,
        host_length_percent_rank_cume_dist,
        LEAST(host_length_percent_rank_cume_dist, host_length_cume_dist) AS least_host_length_percent_rank_cume_dist_host_length_cume_dist,
        COALESCE(k, 'NONE') AS k_filled
    FROM enriched
    WHERE host_length_cume_dist < 0.57
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        k,
        v,
        host_length_cume_dist,
        host_length_rank_2,
        host_length_rank_share,
        host_count_host_length_cume_dist,
        host_length_percent_rank_cume_dist,
        k_filled,
        COALESCE(least_host_length_percent_rank_cume_dist_host_length_cume_dist, 0) AS least_host_length_percent_rank_cume_dist_host_length_cume_dist_filled,
        ROUND(host_length_rank, 2) AS host_length_rank_rounded
    FROM filtered
    WHERE k IS NOT NULL
      AND k IS NOT DISTINCT FROM 'version'
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN host_length_rank_share >= 0.89 THEN 'PREMIUM' ELSE 'STANDARD' END AS host_length_rank_share_tier,
        CASE WHEN host_length_cume_dist >= 0.65 THEN 'PREMIUM' ELSE 'STANDARD' END AS host_length_cume_dist_tier,
        PERCENTILE_APPROX(host_length_rank_share, ARRAY(0.25, 0.5, 0.75)) AS host_length_rank_share_quartiles,
        SIZE(COLLECT_SET(k)) AS num_ks,
        COUNT(DISTINCT k) AS distinct_ks,
        PERCENTILE_APPROX(host_length_rank_rounded, 0.5) AS host_length_rank_rounded_p50,
        SORT_ARRAY(COLLECT_SET(k)) AS k_list,
        PERCENT_RANK() OVER (PARTITION BY CASE WHEN host_length_rank_share >= 0.89 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SIZE(COLLECT_SET(k)) DESC) AS num_ks_percent_rank
    FROM valid
    GROUP BY CASE WHEN host_length_rank_share >= 0.89 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN host_length_cume_dist >= 0.65 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    host_length_rank_share_tier,
    host_length_cume_dist_tier,
    num_ks,
    distinct_ks,
    num_ks_percent_rank
FROM totals
