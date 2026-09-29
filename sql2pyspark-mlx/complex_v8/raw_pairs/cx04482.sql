WITH cleaned_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        CASE WHEN status IN ('ACTIVE', 'EXPIRED') THEN 'group_a' WHEN status = 'LAPSED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM policies
    WHERE product NOT IN ('AUTO', 'HOME')
),
with_parent_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.status_group,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM cleaned_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        SUM(premium) AS total_policy_premium,
        MAX(end_date) AS last_policy_end_date
    FROM policies
    GROUP BY holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.total_policy_premium,
        p.last_policy_end_date
    FROM with_parent_holder s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status_group,
        s.holder_joined_on,
        s.holder_risk_score,
        s.total_policy_premium,
        s.last_policy_end_date,
        p.holder_name,
        p.joined_on AS holder_joined_on_2
    FROM summarized_rows s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        end_date,
        holder_joined_on,
        holder_risk_score,
        last_policy_end_date,
        holder_name,
        holder_joined_on_2,
        LEAD(holder_risk_score, 2) OVER (ORDER BY total_policy_premium DESC, id) AS next2_holder_risk_score,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY YEAR(last_policy_end_date), status_group)) AS distinct_holder_id_in_last_policy_end_date_year_
    FROM enriched_holder
)
SELECT
    id,
    holder_id,
    end_date,
    holder_joined_on,
    holder_risk_score,
    last_policy_end_date,
    holder_name,
    holder_joined_on_2,
    next2_holder_risk_score,
    distinct_holder_id_in_last_policy_end_date_year_,
    ROUND(next2_holder_risk_score / NULLIF(holder_risk_score, 0), 3) AS next2_holder_risk_score_per_holder_risk_score
FROM with_windows
