WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT *
    FROM policies
    WHERE holder_id >= 8
      AND end_date < DATE '2026-03-13'
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.status,
        c.id AS claim_id,
        c.claim_type,
        c.status AS claim_status,
        c.claim_date,
        c.claimed_amount AS claim_claimed_amount
    FROM filtered_policys s
    LEFT JOIN claims c ON c.policy_id = s.id
),
unique_rows AS (
    -- Keep the latest row per claim_status, holder_id
    SELECT
        id,
        holder_id,
        status,
        claim_id,
        claim_type,
        claim_status,
        claim_date,
        claim_claimed_amount
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_status, holder_id ORDER BY claim_claimed_amount DESC, id DESC, claim_id DESC) AS recency_rank
        FROM exploded_detail_claims
    ) ranked
    WHERE recency_rank = 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        status,
        claim_id,
        claim_type,
        claim_status,
        claim_date,
        claim_claimed_amount,
        RANK() OVER (PARTITION BY claim_type ORDER BY claim_date ASC) AS claim_date_rank,
        SIZE(COLLECT_SET(claim_id) OVER (PARTITION BY claim_type, status)) AS distinct_claim_id_in_claim_type_status,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY claim_claimed_amount ASC) AS claim_claimed_amount_dense_rank,
        ROUND(claim_claimed_amount - AVG(claim_claimed_amount) OVER w_claim_type_status, 2) AS claim_claimed_amount_vs_claim_type_status_avg
    FROM unique_rows
    WINDOW w_claim_type_status AS (PARTITION BY claim_type, status)
)
-- Subtotals with GROUPING SETS
SELECT
    holder_id,
    claim_status,
    COUNT(*) FILTER (WHERE claim_type IN ('medical', 'theft', 'flood')) AS n_claim_type_in_medical_theft,
    SUM(claim_claimed_amount_vs_claim_type_status_avg) AS total_claim_claimed_amount_vs_claim_type_status_,
    COUNT(*) AS n_rows
FROM windowed
GROUP BY GROUPING SETS ((holder_id, claim_status), (holder_id), (claim_status), ())
ORDER BY holder_id NULLS LAST, claim_status NULLS LAST
