WITH base AS (
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
    WHERE policy_id IS NOT NULL
      AND reported_at >= TIMESTAMP '2025-01-19 13:00:00'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        MAX(approved_amount) OVER w_adjuster_id_claim_type AS adjuster_id_claim_type_max_approved_amount,
        ROW_NUMBER() OVER (PARTITION BY claim_type, adjuster_id ORDER BY claim_date, id) AS seq_no
    FROM base
    WINDOW w_adjuster_id_claim_type AS (PARTITION BY adjuster_id, claim_type)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(claim_date) AS claim_date_year,
        status,
        policy_id,
        COUNT(*) FILTER (WHERE status IS NOT NULL) AS n_status_is_not_null,
        ROUND(SUM(seq_no) / NULLIF(SUM(claimed_amount), 0), 3) AS seq_no_to_claimed_amount_ratio,
        COUNT(*) AS cnt,
        SUM(claimed_amount) AS total_claimed_amount,
        SUM(CASE WHEN status NOT IN ('PAID', 'OPEN') THEN seq_no ELSE 0 END) AS seq_no_status_not_in_paid_open,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids
    FROM sequenced
    GROUP BY YEAR(claim_date), status, policy_id
    HAVING COUNT(*) >= 3
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE c.status NOT IN ('PAID', 'APPROVED')) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_status_count
    FROM rollup_base s
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(total_claimed_amount * 1.1, 2) AS total_claimed_amount_adj,
        LENGTH(TRIM(status)) AS status_len,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix
    FROM with_counts
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(claim_status_count / NULLIF(SUM(claim_status_count) OVER (PARTITION BY status), 0), 4) AS claim_status_count_share_of_status,
        ROUND(CUME_DIST() OVER (PARTITION BY n_status_is_not_null, distinct_policy_ids ORDER BY total_claimed_amount), 3) AS total_claimed_amount_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY n_status_is_not_null, distinct_policy_ids ORDER BY total_claimed_amount, claim_date_year, status, policy_id), 3) AS total_claimed_amount_percent_rank
    FROM labeled
),
with_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.claim_date_year,
        s.status,
        s.policy_id,
        s.n_status_is_not_null,
        s.cnt,
        s.total_claimed_amount,
        s.seq_no_status_not_in_paid_open,
        s.distinct_policy_ids,
        s.claim_status_count,
        s.status_len,
        s.status_prefix,
        s.claim_status_count_share_of_status,
        s.total_claimed_amount_cume_dist,
        p.product AS policy_product
    FROM windowed s
    JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    claim_date_year,
    status,
    policy_id,
    n_status_is_not_null,
    cnt,
    total_claimed_amount,
    seq_no_status_not_in_paid_open,
    distinct_policy_ids,
    claim_status_count,
    status_prefix,
    claim_status_count_share_of_status,
    total_claimed_amount_cume_dist,
    policy_product
FROM with_details_policy
ORDER BY policy_id DESC, total_claimed_amount DESC
