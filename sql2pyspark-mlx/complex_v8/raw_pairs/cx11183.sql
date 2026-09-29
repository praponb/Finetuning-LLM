WITH base_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE (claim_date >= DATE '2024-07-22' OR approved_amount <= 14993.0)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        ROUND(AVG(approved_amount) OVER w_adjuster_id_policy_id, 2) AS adjuster_id_policy_id_avg_approved_amount,
        ROUND(AVG(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY approved_amount ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_1_prior,
        FIRST_VALUE(claim_date) OVER (PARTITION BY policy_id, adjuster_id ORDER BY approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date,
        MAX(claimed_amount) OVER (PARTITION BY YEAR(claim_date), status ORDER BY claim_date ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS claimed_amount_max_3,
        ROW_NUMBER() OVER (PARTITION BY YEAR(claim_date), status ORDER BY claim_date ASC, id) AS claim_date_seq,
        approved_amount - LAG(approved_amount) OVER (PARTITION BY YEAR(claim_date), status ORDER BY claim_date ASC, id) AS approved_amount_change,
        LAG(claim_date) OVER w_policy_id_adjuster_id_by_app_2 AS prev_claim_date,
        MIN(approved_amount) OVER (PARTITION BY adjuster_id, policy_id) AS adjuster_id_policy_id_min_approved_amount
    FROM base_claims
    WINDOW w_adjuster_id_policy_id AS (PARTITION BY adjuster_id, policy_id), w_policy_id_adjuster_id_by_app_2 AS (PARTITION BY policy_id, adjuster_id ORDER BY approved_amount ASC, id)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        INITCAP(TRIM(claim_type)) AS claim_type_clean,
        ROUND(claimed_amount_max_3 * 1.5, 2) AS claimed_amount_max_3_adj
    FROM sequenced
    WHERE claim_type = 'delay'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claim_date,
        approved_amount,
        reported_at,
        adjuster_id_policy_id_avg_approved_amount,
        approved_amount_avg_1_prior,
        first_claim_date,
        claim_date_seq,
        approved_amount_change,
        adjuster_id_policy_id_min_approved_amount,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_claim_type_claim_date_seq, 0), 4) AS approved_amount_share_of_claim_type_claim_date_s,
        ROUND(approved_amount_change / NULLIF(SUM(approved_amount_change) OVER (PARTITION BY claim_type, claim_date_seq), 0), 4) AS approved_amount_change_share_of_claim_type_claim,
        LAST_VALUE(first_claim_date) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY approved_amount ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_claim_date,
        ROUND(claimed_amount_max_3_adj / NULLIF(SUM(claimed_amount_max_3_adj) OVER (PARTITION BY claim_type, claim_date_seq), 0), 4) AS claimed_amount_max_3_adj_share_of_claim_type_cla,
        RANK() OVER (PARTITION BY policy_id ORDER BY approved_amount ASC NULLS LAST) AS approved_amount_rank
    FROM enriched
    WINDOW w_claim_type_claim_date_seq AS (PARTITION BY claim_type, claim_date_seq)
)
SELECT
    id,
    policy_id,
    claim_date,
    approved_amount,
    reported_at,
    approved_amount_avg_1_prior,
    first_claim_date,
    claim_date_seq,
    approved_amount_change,
    approved_amount_share_of_claim_type_claim_date_s,
    last_known_first_claim_date,
    claimed_amount_max_3_adj_share_of_claim_type_cla,
    approved_amount_rank
FROM metrics
ORDER BY id DESC, claim_date_seq DESC NULLS FIRST
