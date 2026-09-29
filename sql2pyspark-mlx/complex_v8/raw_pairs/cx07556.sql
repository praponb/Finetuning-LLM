WITH base_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        LOWER(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE approved_amount > 0.0
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        ROUND(CUME_DIST() OVER (ORDER BY claim_date DESC, id), 3) AS claim_date_cume_dist,
        RANK() OVER (ORDER BY claim_date DESC) AS claim_date_rank,
        RANK() OVER (ORDER BY claim_date DESC, id) AS claim_date_rank_2,
        LEAD(approved_amount, 3) OVER (PARTITION BY policy_id ORDER BY claim_date, id) AS next3_approved_amount,
        COUNT(*) OVER (PARTITION BY policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_id), 0), 4) AS approved_amount_share_of_policy_id,
        LAG(claim_date, 2) OVER (ORDER BY approved_amount DESC, id) AS prev2_claim_date,
        ROW_NUMBER() OVER w_all_by_approved_amount AS rn,
        FIRST_VALUE(claim_date) OVER (ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        ROUND(AVG(approved_amount) OVER (ORDER BY claim_date DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_4,
        LAST_VALUE(claim_date) OVER (ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date,
        NTH_VALUE(approved_amount, 2) OVER (ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount
    FROM base_claims
    WINDOW w_all_by_approved_amount AS (ORDER BY approved_amount DESC, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM with_windows
    WHERE claim_date_rank <= 5
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    approved_amount,
    claim_date_cume_dist,
    claim_date_rank,
    claim_date_rank_2,
    approved_amount_share_of_policy_id,
    prev2_claim_date,
    first_claim_date,
    approved_amount_avg_4,
    last_claim_date,
    second_approved_amount,
    LENGTH(TRIM(claim_type)) AS claim_type_len
FROM top_per_group
WHERE policy_id >= 16
ORDER BY last_claim_date, id
LIMIT 20
