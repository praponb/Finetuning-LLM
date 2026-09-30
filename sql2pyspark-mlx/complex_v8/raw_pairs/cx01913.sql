WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        status
    FROM policies
    WHERE holder_id >= 9
),
with_parent_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        p.holder_name,
        p.risk_score AS holder_risk_score
    FROM recent_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        product,
        status,
        NTH_VALUE(holder_risk_score, 2) OVER (ORDER BY holder_risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_holder_risk_score,
        ROUND(CUME_DIST() OVER (ORDER BY holder_risk_score), 3) AS holder_risk_score_cume_dist,
        NTILE(5) OVER (ORDER BY holder_risk_score, id) AS holder_risk_score_quintile,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER w_holder_id_status, 0), 4) AS holder_risk_score_share_of_holder_id_status,
        SUM(holder_risk_score) OVER (PARTITION BY holder_id, status) AS holder_id_status_sum_holder_risk_score
    FROM with_parent_holder
    WINDOW w_holder_id_status AS (PARTITION BY holder_id, status)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, holder_risk_score_share_of_holder_id_status, holder_risk_score_cume_dist, second_holder_risk_score, holder_id_status_sum_holder_risk_score
    FROM windowed
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR measure IN (holder_risk_score_share_of_holder_id_status, holder_risk_score_cume_dist, second_holder_risk_score, holder_id_status_sum_holder_risk_score)
) unp
ORDER BY id, measure
