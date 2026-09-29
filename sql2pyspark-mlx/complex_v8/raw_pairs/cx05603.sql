WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at + INTERVAL 1 DAYS AS reported_at_plus_1d
    FROM claims
    WHERE policy_id >= 7
      AND claimed_amount > 19347.2
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM cleaned_claims s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claim_type IN ('delay', 'flood', 'theft')
        )
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.reported_at_plus_1d,
        c.id AS claim_id,
        c.claim_date,
        c.approved_amount AS claim_approved_amount,
        c.policy_id AS claim_policy_id
    FROM screened s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.claim_date BETWEEN DATE '2024-04-27' AND DATE '2025-04-17'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(claimed_amount) OVER (PARTITION BY YEAR(claim_date) ORDER BY claim_date DESC, id, claim_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS claimed_amount_sum_7,
        SIZE(COLLECT_SET(claim_id) OVER (PARTITION BY YEAR(claim_date), policy_id)) AS distinct_claim_id_in_claim_date_year_policy_id,
        MIN(claimed_amount) OVER (PARTITION BY YEAR(claim_date), policy_id) AS claim_date_year_policy_id_min_claimed_amount,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY YEAR(claim_date), policy_id)) AS distinct_claim_type_in_claim_date_year_policy_id,
        NTILE(4) OVER (PARTITION BY YEAR(claim_date) ORDER BY claim_date DESC, id, claim_id) AS claim_date_quartile,
        ROUND(AVG(claim_approved_amount) OVER (PARTITION BY YEAR(claim_date) ORDER BY claim_date DESC, id, claim_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS claim_approved_amount_avg_3_prior
    FROM line_level
)
-- Subtotals with CUBE
SELECT
    adjuster_id,
    claim_id,
    claim_date_quartile,
    MAX(claim_approved_amount_avg_3_prior) AS max_claim_approved_amount_avg_3_prior,
    COUNT(*) AS row_count,
    COUNT(DISTINCT claim_policy_id) AS distinct_claim_policy_ids
FROM sequenced
GROUP BY CUBE(adjuster_id, claim_id, claim_date_quartile)
ORDER BY adjuster_id NULLS LAST, claim_id NULLS LAST, claim_date_quartile NULLS LAST
