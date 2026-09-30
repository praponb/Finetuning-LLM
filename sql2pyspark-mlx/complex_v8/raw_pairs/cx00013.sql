WITH prepared_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        ABS(premium - 2289.0) AS premium_distance
    FROM policies
    WHERE start_date IS NOT NULL
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM prepared_policys s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.status = 'REJECTED'
              AND c.approved_amount < 14377.6
        )
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment
    FROM screened s
    JOIN policyholders p ON p.id = s.holder_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(premium_distance - AVG(premium_distance) OVER w_holder_id, 2) AS premium_distance_vs_holder_id_avg,
        ROUND(holder_risk_score - AVG(holder_risk_score) OVER (PARTITION BY holder_id), 2) AS holder_risk_score_vs_holder_id_avg
    FROM combined_details
    WINDOW w_holder_id AS (PARTITION BY holder_id)
),
latest_per_key AS (
    -- Keep the latest row per status
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        days_start_date_to_end_date,
        premium_distance,
        holder_joined_on,
        holder_risk_score,
        holder_segment,
        premium_distance_vs_holder_id_avg,
        holder_risk_score_vs_holder_id_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC, id DESC) AS recency_rank
        FROM with_windows
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    holder_id,
    status,
    days_start_date_to_end_date,
    premium_distance,
    holder_risk_score,
    premium_distance_vs_holder_id_avg,
    holder_risk_score_vs_holder_id_avg,
    CASE WHEN premium >= 3532.7 THEN 'A' WHEN premium >= 2033.8 THEN 'B' ELSE 'C' END AS premium_tier,
    LENGTH(TRIM(status)) AS status_len
FROM latest_per_key
ORDER BY id, holder_id
