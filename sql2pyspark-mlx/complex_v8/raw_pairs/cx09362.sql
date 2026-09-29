WITH recent_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        risk_score,
        joined_on,
        segment,
        CONCAT_WS(': ', holder_name, region) AS holder_name_region_label,
        CASE WHEN segment IN ('SME', 'RETAIL') THEN 'group_a' WHEN segment = 'VIP' THEN 'group_b' ELSE 'other' END AS segment_group
    FROM policyholders
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(risk_score) OVER (PARTITION BY segment_group ORDER BY joined_on DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS risk_score_avg_3_prior,
        FIRST_VALUE(risk_score) IGNORE NULLS OVER (PARTITION BY segment_group ORDER BY risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_risk_score,
        FIRST_VALUE(joined_on) OVER (PARTITION BY segment ORDER BY joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_joined_on,
        MAX(risk_score) OVER w_segment AS segment_max_risk_score,
        SIZE(COLLECT_SET(segment_group) OVER (PARTITION BY segment)) AS distinct_segment_group_in_segment,
        ROUND(AVG(risk_score) OVER w_segment, 2) AS segment_avg_risk_score,
        MAX(risk_score) OVER (PARTITION BY segment_group) AS segment_group_max_risk_score,
        NTILE(3) OVER (PARTITION BY segment_group ORDER BY joined_on, id) AS joined_on_tercile
    FROM recent_holders
    WINDOW w_segment AS (PARTITION BY segment)
),
per_group AS (
    -- Summarise per group
    SELECT
        segment_group,
        DATE_TRUNC('MONTH', joined_on) AS joined_on_month,
        distinct_segment_group_in_segment,
        ROUND(SUM(segment_group_max_risk_score) / NULLIF(SUM(risk_score), 0), 3) AS segment_group_max_risk_score_to_risk_score_ratio,
        SUM(CASE WHEN segment <> 'RETAIL' THEN 1 ELSE 0 END) AS n_segment_retail
    FROM sequenced
    GROUP BY segment_group, DATE_TRUNC('MONTH', joined_on), distinct_segment_group_in_segment
    HAVING SUM(risk_score) > 0.1
)
SELECT
    segment_group,
    joined_on_month,
    segment_group_max_risk_score_to_risk_score_ratio,
    n_segment_retail
FROM per_group
ORDER BY joined_on_month
