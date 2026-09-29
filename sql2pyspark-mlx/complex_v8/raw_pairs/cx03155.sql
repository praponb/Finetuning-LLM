WITH filtered_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        ROUND(installed_kw * 1.1, 2) AS installed_kw_adj,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket
    FROM sites
    WHERE zone = 'Z4'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        installed_kw_adj,
        installed_kw_bucket,
        FIRST_VALUE(installed_kw) OVER (PARTITION BY tariff ORDER BY installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_installed_kw,
        FIRST_VALUE(commissioned_on) IGNORE NULLS OVER (ORDER BY installed_kw_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_commissioned_on,
        SIZE(COLLECT_SET(zone) OVER w_tariff) AS distinct_zone_in_tariff,
        COUNT(*) OVER (PARTITION BY tariff ORDER BY installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(4) OVER (ORDER BY installed_kw_bucket ASC, id) AS installed_kw_bucket_quartile,
        LAG(installed_kw_bucket, 2) OVER (ORDER BY installed_kw_bucket DESC, id) AS prev2_installed_kw_bucket
    FROM filtered_sites
    WINDOW w_tariff AS (PARTITION BY tariff)
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
        )
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_zone_in_tariff,
        ROUND(100 * COUNT(CASE WHEN zone = 'Z4' THEN 1 END) / COUNT(*), 1) AS pct_zone_z4,
        COUNT(*) AS cnt
    FROM with_activity
    GROUP BY distinct_zone_in_tariff
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        distinct_zone_in_tariff,
        pct_zone_z4,
        DENSE_RANK() OVER (PARTITION BY distinct_zone_in_tariff, cnt ORDER BY cnt, distinct_zone_in_tariff) AS cnt_dense_rank,
        LEAD(distinct_zone_in_tariff, 2) OVER w_distinct_zone_in_tariff_cnt AS next2_distinct_zone_in_tariff,
        DENSE_RANK() OVER (PARTITION BY distinct_zone_in_tariff, cnt ORDER BY cnt) AS cnt_dense_rank_2,
        ROUND(pct_zone_z4 / NULLIF(SUM(pct_zone_z4) OVER (PARTITION BY cnt), 0), 4) AS pct_zone_z4_share_of_cnt,
        ROUND(distinct_zone_in_tariff - AVG(distinct_zone_in_tariff) OVER (PARTITION BY cnt), 2) AS distinct_zone_in_tariff_vs_cnt_avg,
        ROW_NUMBER() OVER (PARTITION BY distinct_zone_in_tariff, cnt ORDER BY cnt, distinct_zone_in_tariff) AS cnt_seq
    FROM grouped
    WINDOW w_distinct_zone_in_tariff_cnt AS (PARTITION BY distinct_zone_in_tariff, cnt ORDER BY cnt, distinct_zone_in_tariff)
),
labeled AS (
    -- Derive helper columns
    SELECT
        distinct_zone_in_tariff,
        pct_zone_z4,
        cnt_dense_rank,
        next2_distinct_zone_in_tariff,
        cnt_dense_rank_2,
        pct_zone_z4_share_of_cnt,
        ABS(cnt_dense_rank_2 - 7) AS cnt_dense_rank_2_distance
    FROM with_windows
    WHERE distinct_zone_in_tariff_vs_cnt_avg <= 7.0
)
SELECT
    distinct_zone_in_tariff,
    pct_zone_z4,
    cnt_dense_rank_2,
    pct_zone_z4_share_of_cnt,
    cnt_dense_rank_2_distance,
    ABS(pct_zone_z4 - 45.0) AS pct_zone_z4_distance
FROM labeled
WHERE cnt_dense_rank BETWEEN 4 AND 8
ORDER BY distinct_zone_in_tariff NULLS LAST
