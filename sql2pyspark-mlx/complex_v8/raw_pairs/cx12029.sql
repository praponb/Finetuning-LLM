WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date
    FROM policies
    WHERE product IS NOT NULL
      AND status IN ('EXPIRED', 'LAPSED')
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        c.id AS claim_id,
        c.claim_date,
        c.adjuster_id AS claim_adjuster_id
    FROM valid s
    LEFT JOIN claims c ON c.policy_id = s.id
),
latest AS (
    -- Keep the latest row per claim_adjuster_id
    SELECT
        id,
        holder_id,
        product,
        premium,
        claim_id,
        claim_date,
        claim_adjuster_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_adjuster_id ORDER BY premium DESC, id DESC, claim_id DESC) AS row_num
        FROM with_children
    ) ranked
    WHERE row_num = 1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (ORDER BY premium ASC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(premium) OVER (PARTITION BY claim_id ORDER BY claim_date, id, claim_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_1_prior,
        DATEDIFF(claim_date, LAG(claim_date) OVER (ORDER BY premium ASC, id, claim_id)) AS days_since_prev_claim_date,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (ORDER BY claim_date DESC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        ROUND(AVG(premium) OVER w_holder_id, 2) AS holder_id_avg_premium
    FROM latest
    WINDOW w_holder_id AS (PARTITION BY holder_id)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.claim_id,
        s.claim_date,
        s.claim_adjuster_id,
        s.running_count,
        s.premium_avg_1_prior,
        s.last_known_claim_date,
        s.holder_id_avg_premium,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment,
        a.team AS adjuster_team
    FROM with_trends s
    LEFT JOIN policyholders p ON p.id = s.holder_id
    LEFT JOIN adjusters a ON a.id = s.claim_adjuster_id
)
SELECT
    id,
    holder_id,
    product,
    premium,
    claim_id,
    running_count,
    last_known_claim_date,
    holder_risk_score,
    holder_segment,
    adjuster_team,
    DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label
FROM combined_details
ORDER BY running_count DESC, id, claim_id
LIMIT 20
