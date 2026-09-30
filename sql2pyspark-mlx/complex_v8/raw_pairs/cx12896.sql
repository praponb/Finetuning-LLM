WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        claimed_amount + approved_amount AS claimed_amount_plus_approved_amount,
        CONCAT_WS(': ', status, claim_type) AS status_claim_type_label
    FROM claims
    WHERE claim_date BETWEEN DATE '2024-02-19' AND DATE '2025-04-20'
),
summary AS (
    -- Group-level totals
    SELECT
        claim_type,
        COUNT(*) FILTER (WHERE claim_type IS NOT NULL) AS n_claim_type_is_not_null,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        COUNT(*) FILTER (WHERE status <> 'IN_REVIEW') AS n_status_in_review,
        ROUND(AVG(claimed_amount_plus_approved_amount), 2) AS avg_claimed_amount_plus_approved_amount,
        SUM(claimed_amount_plus_approved_amount) AS total_claimed_amount_plus_approved_amount
    FROM filtered_claims
    GROUP BY claim_type
    HAVING SUM(claimed_amount) > 4234.9
),
tagged AS (
    -- Add calculated columns
    SELECT
        claim_type,
        n_claim_type_is_not_null,
        n_status_in_review,
        avg_claimed_amount_plus_approved_amount,
        total_claimed_amount_plus_approved_amount,
        LEAST(n_status_in_review, n_claim_type_is_not_null) AS least_n_status_in_review_n_claim_type_is_not_nul,
        ABS(n_status_in_review - 6) AS n_status_in_review_distance
    FROM summary
),
labeled AS (
    -- Derive helper columns
    SELECT
        claim_type,
        total_claimed_amount_plus_approved_amount,
        UPPER(TRIM(claim_type)) AS claim_type_clean,
        COALESCE(n_status_in_review, 0) AS n_status_in_review_filled,
        CASE WHEN n_status_in_review >= 8 THEN 'A' WHEN n_status_in_review >= 4 THEN 'B' ELSE 'C' END AS n_status_in_review_tier
    FROM tagged
    WHERE total_claimed_amount_plus_approved_amount >= 180000.0
),
derived AS (
    -- Add calculated columns
    SELECT
        claim_type,
        total_claimed_amount_plus_approved_amount,
        claim_type_clean,
        n_status_in_review_filled,
        LENGTH(TRIM(n_status_in_review_tier)) AS n_status_in_review_tier_len
    FROM labeled
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_claim_type_by_n_status_in_re, 3) AS n_status_in_review_tier_len_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type ORDER BY n_status_in_review_tier_len), 3) AS n_status_in_review_tier_len_percent_rank,
        MAX(n_status_in_review_tier_len) OVER (PARTITION BY claim_type ORDER BY n_status_in_review_tier_len, claim_type ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS n_status_in_review_tier_len_max_1_prior,
        ROUND(n_status_in_review_tier_len - AVG(n_status_in_review_tier_len) OVER (PARTITION BY n_status_in_review_tier_len), 2) AS n_status_in_review_tier_len_vs_n_status_in_revie,
        COUNT(*) OVER (PARTITION BY n_status_in_review_tier_len) AS rows_in_n_status_in_review_tier_len
    FROM derived
    WINDOW w_claim_type_by_n_status_in_re AS (PARTITION BY claim_type ORDER BY n_status_in_review_tier_len)
)
-- Final projection
SELECT DISTINCT
    total_claimed_amount_plus_approved_amount,
    n_status_in_review_tier_len_percent_rank,
    n_status_in_review_tier_len_vs_n_status_in_revie
FROM ranked
ORDER BY total_claimed_amount_plus_approved_amount, n_status_in_review_tier_len_percent_rank DESC
