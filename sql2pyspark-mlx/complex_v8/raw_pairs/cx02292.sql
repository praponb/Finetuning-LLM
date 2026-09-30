WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment
    FROM policyholders
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        SIZE(COLLECT_SET(region) OVER (PARTITION BY YEAR(joined_on))) AS distinct_region_in_joined_on_year,
        risk_score - LAG(risk_score) OVER (ORDER BY risk_score, id) AS risk_score_change,
        ROUND(risk_score - AVG(risk_score) OVER (PARTITION BY YEAR(joined_on)), 2) AS risk_score_vs_joined_on_year_avg,
        ROW_NUMBER() OVER w_all_by_risk_score AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY segment ORDER BY joined_on DESC), 3) AS joined_on_cume_dist
    FROM base
    WINDOW w_all_by_risk_score AS (ORDER BY risk_score, id)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        risk_score_change - LAG(risk_score_change) OVER w_all_by_distinct_region_in_jo AS risk_score_change_change,
        MIN(risk_score) OVER w_seq_no_distinct_region_in_jo AS seq_no_distinct_region_in_joined_on_year_min_ris,
        NTH_VALUE(joined_on_cume_dist, 2) OVER (ORDER BY distinct_region_in_joined_on_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_joined_on_cume_dist,
        NTH_VALUE(joined_on, 2) OVER (ORDER BY distinct_region_in_joined_on_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_joined_on,
        NTILE(3) OVER (PARTITION BY distinct_region_in_joined_on_year ORDER BY distinct_region_in_joined_on_year DESC, id) AS distinct_region_in_joined_on_year_tercile
    FROM with_trends
    WINDOW w_all_by_distinct_region_in_jo AS (ORDER BY distinct_region_in_joined_on_year DESC, id), w_seq_no_distinct_region_in_jo AS (PARTITION BY seq_no, distinct_region_in_joined_on_year)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE seq_no = 1
)
SELECT
    id,
    holder_name,
    region,
    risk_score,
    joined_on,
    segment,
    distinct_region_in_joined_on_year,
    risk_score_change,
    risk_score_vs_joined_on_year_avg,
    seq_no,
    joined_on_cume_dist,
    risk_score_change_change,
    second_joined_on
FROM top_per_group
WHERE distinct_region_in_joined_on_year < 8
ORDER BY risk_score DESC NULLS FIRST, joined_on DESC
