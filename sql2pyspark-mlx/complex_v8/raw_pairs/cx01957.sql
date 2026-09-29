WITH prepared_holders AS (
    -- Filter policyholders
    SELECT
        id,
        region,
        risk_score,
        segment,
        LAST_DAY(joined_on) AS joined_on_month_end
    FROM policyholders
    WHERE region IS NOT NULL
),
with_activity AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM prepared_holders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.premium < 4417.0
        )
),
line_level AS (
    -- One row per policy
    SELECT
        s.id,
        s.risk_score,
        s.joined_on_month_end,
        p.id AS policy_id,
        p.status AS policy_status,
        p.end_date AS policy_end_date
    FROM with_activity s
    LEFT JOIN policies p ON p.holder_id = s.id AND p.status IN ('LAPSED', 'ACTIVE', 'CANCELLED')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(joined_on_month_end), YEAR(policy_end_date) ORDER BY policy_end_date DESC), 3) AS policy_end_date_percent_rank,
        ROUND(risk_score - AVG(risk_score) OVER (PARTITION BY YEAR(joined_on_month_end), policy_status), 2) AS risk_score_vs_joined_on_month_end_year_policy_st,
        ROUND(AVG(risk_score) OVER (PARTITION BY MONTH(joined_on_month_end), YEAR(policy_end_date) ORDER BY policy_end_date DESC, id, policy_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS risk_score_avg_2,
        COUNT(*) OVER (PARTITION BY YEAR(joined_on_month_end), policy_status) AS rows_in_joined_on_month_end_year_policy_status
    FROM line_level
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(policy_status), 1, 2) AS policy_status_prefix,
        LOWER(TRIM(policy_status)) AS policy_status_clean,
        CONCAT(policy_status, '#', CAST(id AS STRING)) AS policy_status_ref
    FROM with_trends
    WHERE risk_score BETWEEN 0.1 AND 0.7
)
SELECT id, policy_id, measure, value
FROM (
    SELECT id, policy_id, risk_score_avg_2, risk_score_vs_joined_on_month_end_year_policy_st, risk_score, policy_end_date_percent_rank
    FROM labeled
) src
UNPIVOT (
    value FOR measure IN (risk_score_avg_2 AS risk_score_avg_2_v, risk_score_vs_joined_on_month_end_year_policy_st AS risk_score_vs_joined_on_month_end_year_policy_st_2, risk_score AS m_risk_score, policy_end_date_percent_rank AS m_policy_end_date_percent_rank)
) unp
