WITH cleaned_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        LENGTH(TRIM(holder_name)) AS holder_name_len
    FROM policyholders
    WHERE region IN ('Central', 'South', 'North')
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.id) AS policy_count,
        (SELECT MIN(p.start_date) FROM policies p WHERE p.holder_id = s.id) AS first_policy_start_date,
        (SELECT MAX(p.end_date) FROM policies p WHERE p.holder_id = s.id) AS last_policy_end_date
    FROM cleaned_holders s
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(risk_score) OVER (PARTITION BY region ORDER BY joined_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_risk_score_so_far,
        NTH_VALUE(joined_on, 2) OVER (PARTITION BY region ORDER BY joined_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_joined_on,
        SIZE(COLLECT_SET(region) OVER w_holder_name_len_policy_count) AS distinct_region_in_holder_name_len_policy_count,
        RANK() OVER (PARTITION BY region ORDER BY joined_on ASC) AS joined_on_rank,
        SUM(policy_count) OVER (PARTITION BY holder_name_len, policy_count) AS holder_name_len_policy_count_sum_policy_count,
        ROUND(PERCENT_RANK() OVER (PARTITION BY region ORDER BY joined_on ASC, id), 3) AS joined_on_percent_rank
    FROM profiled
    WINDOW w_holder_name_len_policy_count AS (PARTITION BY holder_name_len, policy_count)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, metric_value
FROM (
    SELECT id, min_risk_score_so_far, joined_on_percent_rank, risk_score
    FROM windowed
) src
UNPIVOT (
    metric_value FOR metric IN (min_risk_score_so_far AS m_min_risk_score_so_far, joined_on_percent_rank AS joined_on_percent_rank_v, risk_score AS m_risk_score)
) unp
