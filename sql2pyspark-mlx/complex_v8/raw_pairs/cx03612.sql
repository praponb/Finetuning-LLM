WITH base AS (
    -- Scope the policyholders rows
    SELECT *
    FROM policyholders
    WHERE joined_on < DATE '2025-04-20'
),
enriched AS (
    -- Add calculated columns
    SELECT *, ROUND(risk_score * 0.9, 2) AS risk_score_adj
    FROM base
),
exploded_detail AS (
    -- One row per policy
    SELECT
        s.id,
        s.holder_name,
        s.region,
        s.risk_score,
        s.risk_score_adj,
        p.id AS policy_id,
        p.start_date AS policy_start_date,
        p.end_date AS policy_end_date,
        p.premium AS policy_premium,
        p.status AS policy_status
    FROM enriched s
    JOIN policies p ON p.holder_id = s.id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_name,
        risk_score,
        risk_score_adj,
        policy_id,
        policy_start_date,
        policy_end_date,
        policy_premium,
        policy_status,
        NTH_VALUE(policy_end_date, 2) OVER (PARTITION BY YEAR(policy_start_date) ORDER BY risk_score DESC, id, policy_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_policy_end_date,
        ROUND(AVG(policy_premium) OVER (PARTITION BY YEAR(policy_start_date) ORDER BY risk_score DESC, id, policy_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS policy_premium_avg_2,
        COUNT(*) OVER (PARTITION BY policy_id, MONTH(policy_end_date)) AS rows_in_policy_id_policy_end_date_month,
        LAST_VALUE(risk_score) OVER (PARTITION BY YEAR(policy_start_date) ORDER BY risk_score DESC, id, policy_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_risk_score,
        MAX(risk_score_adj) OVER (PARTITION BY policy_id, MONTH(policy_end_date)) AS policy_id_policy_end_date_month_max_risk_score_a
    FROM exploded_detail
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.status <> 'PAID'
        )
)
SELECT
    id,
    holder_name,
    risk_score,
    risk_score_adj,
    policy_id,
    policy_end_date,
    policy_premium,
    policy_status,
    second_policy_end_date,
    policy_premium_avg_2,
    rows_in_policy_id_policy_end_date_month,
    policy_id_policy_end_date_month_max_risk_score_a,
    policy_premium_avg_2 + risk_score AS policy_premium_avg_2_plus_risk_score
FROM qualified
ORDER BY policy_status DESC, policy_end_date
