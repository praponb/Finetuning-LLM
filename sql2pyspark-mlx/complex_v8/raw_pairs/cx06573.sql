WITH filtered_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE policy_id IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type ORDER BY claimed_amount DESC), 3) AS claimed_amount_percent_rank,
        DENSE_RANK() OVER (PARTITION BY claim_type ORDER BY claimed_amount DESC, id) AS claimed_amount_dense_rank,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY claim_type, adjuster_id ORDER BY claimed_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount,
        LAG(claimed_amount) OVER (PARTITION BY claim_type, adjuster_id ORDER BY claimed_amount DESC NULLS FIRST, id) AS prev_claimed_amount
    FROM filtered_claims
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        claimed_amount_percent_rank,
        claimed_amount_dense_rank,
        last_known_claimed_amount,
        prev_claimed_amount,
        claimed_amount_dense_rank - LAG(claimed_amount_dense_rank) OVER (PARTITION BY policy_id ORDER BY reported_at, id) AS claimed_amount_dense_rank_change,
        NTILE(10) OVER (PARTITION BY claim_type ORDER BY reported_at, id) AS reported_at_decile,
        SUM(claimed_amount_dense_rank) OVER (PARTITION BY policy_id ORDER BY reported_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS claimed_amount_dense_rank_sum_4,
        LEAD(prev_claimed_amount) OVER (PARTITION BY policy_id ORDER BY reported_at, id) AS next_prev_claimed_amount,
        LEAD(claimed_amount_percent_rank) OVER (PARTITION BY policy_id ORDER BY reported_at, id) AS next_claimed_amount_percent_rank,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY claim_type, claimed_amount_dense_rank), 2) AS approved_amount_vs_claim_type_claimed_amount_den
    FROM sequenced
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.claimed_amount_percent_rank,
        s.prev_claimed_amount,
        s.reported_at_decile,
        s.next_prev_claimed_amount,
        s.next_claimed_amount_percent_rank,
        s.approved_amount_vs_claim_type_claimed_amount_den,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.approved_amount AS claim_approved_amount
    FROM with_windows s
    JOIN claims c ON c.policy_id = s.policy_id
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.claimed_amount_percent_rank,
        s.prev_claimed_amount,
        s.reported_at_decile,
        s.next_prev_claimed_amount,
        s.next_claimed_amount_percent_rank,
        s.approved_amount_vs_claim_type_claimed_amount_den,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_approved_amount,
        c.approved_amount AS claim_approved_amount_2,
        c.reported_at AS claim_reported_at,
        c.status AS claim_status
    FROM with_children_claims s
    JOIN claims c ON c.id = s.claim_id
    WHERE c.status NOT IN ('PAID', 'OPEN')
),
combined_details_claim AS (
    -- Join claims
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.claimed_amount_percent_rank,
        s.prev_claimed_amount,
        s.reported_at_decile,
        s.next_prev_claimed_amount,
        s.next_claimed_amount_percent_rank,
        s.approved_amount_vs_claim_type_claimed_amount_den,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_reported_at,
        s.claim_status,
        c.adjuster_id AS claim_adjuster_id_2
    FROM with_details s
    JOIN claims c ON c.id = s.claim_id
),
per_group AS (
    -- Summarise per group
    SELECT
        claim_status,
        ROUND(100 * COUNT(CASE WHEN claim_status <> 'OPEN' THEN 1 END) / COUNT(*), 1) AS pct_claim_status_open,
        ROUND(STDDEV(prev_claimed_amount), 2) AS stddev_prev_claimed_amount,
        MIN(next_prev_claimed_amount) AS min_next_prev_claimed_amount,
        COUNT(*) AS cnt,
        MIN(claim_reported_at) AS first_claim_reported_at
    FROM combined_details_claim
    GROUP BY 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT cnt, first_claim_reported_at, claim_status, pct_claim_status_open
    FROM per_group
) src
PIVOT (
    MAX(pct_claim_status_open)
    FOR claim_status IN ('OPEN', 'APPROVED')
)
ORDER BY cnt, first_claim_reported_at
