WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE start_date >= DATE '2024-06-19'
      AND holder_id IS NOT NULL
),
deduped AS (
    -- Keep the earliest row per holder_id
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY start_date, id) AS row_num
        FROM filtered_policys
    ) ranked
    WHERE row_num = 1
),
with_details_holder AS (
    -- Join policyholders
    SELECT s.*, p.joined_on AS holder_joined_on
    FROM deduped s
    JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY holder_joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium,
        ROUND(CUME_DIST() OVER (ORDER BY end_date DESC), 3) AS end_date_cume_dist,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY holder_joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        FIRST_VALUE(end_date) OVER (PARTITION BY holder_id ORDER BY holder_joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date
    FROM with_details_holder
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.holder_joined_on,
        s.first_known_premium,
        s.first_known_start_date,
        s.first_end_date,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.adjuster_id AS claim_adjuster_id
    FROM with_trends s
    JOIN claims c ON c.policy_id = s.id AND TO_DATE(c.reported_at) < DATE '2025-03-02'
),
latest AS (
    -- Keep the latest row per holder_id, claim_adjuster_id
    SELECT
        id,
        holder_id,
        start_date,
        holder_joined_on,
        first_known_premium,
        first_known_start_date,
        first_end_date,
        claim_id,
        claim_reported_at,
        claim_adjuster_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, claim_adjuster_id ORDER BY first_known_premium DESC, id DESC, claim_id DESC) AS row_num
        FROM detail_rows_claims
    ) ranked
    WHERE row_num = 1
),
combined_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.holder_joined_on,
        s.first_known_start_date,
        s.claim_id,
        s.claim_adjuster_id,
        p.risk_score AS holder_risk_score,
        a.adjuster_name
    FROM latest s
    JOIN policyholders p ON p.id = s.holder_id
    LEFT JOIN adjusters a ON a.id = s.claim_adjuster_id
)
-- Final projection
SELECT
    id,
    start_date,
    holder_joined_on,
    holder_risk_score,
    adjuster_name,
    LOWER(REGEXP_REPLACE(TRIM(adjuster_name), ' ', '_')) AS adjuster_name_slug
FROM combined_details_holder
ORDER BY adjuster_name_slug DESC
