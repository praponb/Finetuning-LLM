WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        SUBSTRING(TRIM(segment), 1, 3) AS segment_prefix,
        DATEDIFF(DATE '2026-01-01', joined_on) AS days_since_joined_on,
        DATE_FORMAT(joined_on, 'yyyy-MM-dd') AS joined_on_label
    FROM policyholders
    WHERE risk_score <= 0.6
),
deduped AS (
    -- Keep the earliest row per segment, region
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        segment_prefix,
        days_since_joined_on,
        joined_on_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY segment, region ORDER BY risk_score, id) AS row_num
        FROM scoped
    ) ranked
    WHERE row_num = 1
),
with_children_policys AS (
    -- One row per policy
    SELECT
        s.id,
        s.holder_name,
        s.region,
        s.risk_score,
        s.segment_prefix,
        s.days_since_joined_on,
        s.joined_on_label,
        p.id AS policy_id,
        p.status AS policy_status,
        p.product AS policy_product,
        p.end_date AS policy_end_date,
        p.start_date AS policy_start_date
    FROM deduped s
    JOIN policies p ON p.holder_id = s.id
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(policy_status, '#', CAST(days_since_joined_on AS STRING)) AS policy_status_ref,
        DATE_FORMAT(policy_end_date, 'yyyy-MM') AS policy_end_date_label
    FROM with_children_policys
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on_label,
        policy_id,
        policy_status,
        policy_start_date,
        policy_end_date_label,
        ROUND(days_since_joined_on - AVG(days_since_joined_on) OVER (PARTITION BY policy_id), 2) AS days_since_joined_on_vs_policy_id_avg,
        ROUND(AVG(risk_score) OVER (PARTITION BY policy_id), 2) AS policy_id_avg_risk_score,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id
    FROM labeled
)
SELECT
    id,
    region,
    risk_score,
    joined_on_label,
    policy_id,
    policy_status,
    policy_start_date,
    policy_id_avg_risk_score,
    rows_in_policy_id
FROM metrics
