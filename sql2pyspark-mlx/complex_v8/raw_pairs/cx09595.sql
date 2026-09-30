WITH recent_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        DAYOFWEEK(joined_on) AS joined_on_dayofweek,
        ROUND(risk_score * 0.25, 2) AS risk_score_adj,
        ROUND(risk_score * 1.1, 2) AS risk_score_adj_2
    FROM policyholders
    WHERE holder_name IS NOT NULL
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        joined_on_dayofweek,
        DATE_FORMAT(joined_on, 'yyyy-MM') AS joined_on_ym,
        segment,
        COUNT(*) AS cnt,
        MAX(risk_score) AS max_risk_score,
        MIN(joined_on) AS first_joined_on,
        SUM(CASE WHEN region = 'South' THEN 1 ELSE 0 END) AS n_region_south,
        COUNT(DISTINCT region) AS distinct_regions
    FROM recent_holders
    GROUP BY joined_on_dayofweek, DATE_FORMAT(joined_on, 'yyyy-MM'), segment
),
derived AS (
    -- Derive helper columns
    SELECT *, LEAST(cnt, n_region_south) AS least_cnt_n_region_south
    FROM rollup_base
    WHERE distinct_regions <= 8
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(cnt, 2) OVER (ORDER BY cnt, joined_on_dayofweek, joined_on_ym, segment ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_cnt,
        NTILE(5) OVER w_all_by_cnt AS cnt_quintile,
        COUNT(*) OVER (PARTITION BY n_region_south, least_cnt_n_region_south) AS rows_in_n_region_south_least_cnt_n_region_south
    FROM derived
    WINDOW w_all_by_cnt AS (ORDER BY cnt, joined_on_dayofweek, joined_on_ym, segment)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_trends
    WHERE n_region_south > (SELECT AVG(n_region_south) FROM with_trends)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        joined_on_dayofweek,
        joined_on_ym,
        segment,
        cnt,
        max_risk_score,
        distinct_regions,
        least_cnt_n_region_south,
        cnt_quintile,
        MIN(cnt) OVER (PARTITION BY joined_on_dayofweek) AS joined_on_dayofweek_min_cnt,
        SIZE(COLLECT_SET(segment) OVER (PARTITION BY joined_on_dayofweek)) AS distinct_segment_in_joined_on_dayofweek,
        DENSE_RANK() OVER w_n_region_south_cnt_by_second AS second_cnt_dense_rank
    FROM filtered
    WINDOW w_n_region_south_cnt_by_second AS (PARTITION BY n_region_south, cnt ORDER BY second_cnt ASC, joined_on_dayofweek, joined_on_ym, segment)
)
-- Final projection
SELECT
    joined_on_dayofweek,
    joined_on_ym,
    segment,
    cnt,
    max_risk_score,
    least_cnt_n_region_south,
    cnt_quintile,
    joined_on_dayofweek_min_cnt,
    distinct_segment_in_joined_on_dayofweek,
    ROUND(max_risk_score * 1.5, 2) AS max_risk_score_adj
FROM with_windows
ORDER BY max_risk_score, joined_on_dayofweek, joined_on_ym, segment
LIMIT 5
