WITH cleaned_claims AS (
    -- Filter claims
    SELECT *
    FROM claims
    WHERE status = 'APPROVED'
      AND adjuster_id <= 4
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.status,
        c.id AS claim_id,
        c.claim_date AS claim_claim_date,
        c.policy_id AS claim_policy_id
    FROM cleaned_claims s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        status,
        claim_id,
        claim_claim_date,
        claim_policy_id,
        RANK() OVER w_all_by_claim_claim_date_ties AS claim_claim_date_rank,
        ROW_NUMBER() OVER w_all_by_claim_claim_date AS claim_claim_date_seq,
        NTH_VALUE(claim_claim_date, 2) OVER (PARTITION BY claim_type, claim_policy_id ORDER BY claim_claim_date, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_claim_date,
        RANK() OVER w_claim_type_claim_policy_id_b_2 AS claim_claim_date_rank_2,
        FIRST_VALUE(claim_claim_date) OVER (ORDER BY claim_claim_date ASC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_claim_date
    FROM with_children
    WINDOW w_all_by_claim_claim_date_ties AS (ORDER BY claim_claim_date ASC), w_all_by_claim_claim_date AS (ORDER BY claim_claim_date ASC, id, claim_id), w_claim_type_claim_policy_id_b_2 AS (PARTITION BY claim_type, claim_policy_id ORDER BY claim_claim_date, id, claim_id)
),
grouped AS (
    -- Summarise per group
    SELECT
        claim_claim_date_rank_2,
        DATE_TRUNC('MONTH', first_claim_claim_date) AS first_claim_claim_date_month,
        DATE_FORMAT(claim_claim_date, 'yyyy-MM') AS claim_claim_date_ym,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE status IN ('REJECTED', 'IN_REVIEW', 'PAID')) AS n_status_in_rejected_in_review,
        ROUND(SUM(claim_claim_date_rank_2) / NULLIF(SUM(claim_claim_date_seq), 0), 3) AS claim_claim_date_rank_2_to_claim_claim_date_seq_,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_trends
    GROUP BY claim_claim_date_rank_2, DATE_TRUNC('MONTH', first_claim_claim_date), DATE_FORMAT(claim_claim_date, 'yyyy-MM')
    HAVING COUNT(*) >= 2
)
SELECT
    claim_claim_date_rank_2,
    claim_claim_date_ym,
    cnt,
    n_status_in_rejected_in_review,
    claim_claim_date_rank_2_to_claim_claim_date_seq_,
    LOWER(REGEXP_REPLACE(TRIM(claim_claim_date_ym), ' ', '_')) AS claim_claim_date_ym_slug
FROM grouped
WHERE claim_claim_date_rank_2 < 9
