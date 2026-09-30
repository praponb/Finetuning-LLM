WITH prepared AS (
    -- Scope the claims rows
    SELECT *
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(claim_type, '#', CAST(policy_id AS STRING)) AS claim_type_ref,
        DAYOFWEEK(claim_date) AS claim_date_dayofweek
    FROM prepared
    WHERE adjuster_id >= 6
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count
    FROM enriched s
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at,
        claim_type_ref,
        ROUND(claim_count - AVG(claim_count) OVER w_claim_count, 2) AS claim_count_vs_claim_count_avg,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY claim_count), 2) AS approved_amount_vs_claim_count_avg,
        MIN(claim_count) OVER (PARTITION BY claim_count) AS claim_count_min_claim_count,
        NTILE(3) OVER (PARTITION BY claim_date_dayofweek ORDER BY claim_date DESC, id) AS claim_date_tercile
    FROM with_counts
    WINDOW w_claim_count AS (PARTITION BY claim_count)
),
joined AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claim_count_vs_claim_count_avg,
        s.approved_amount_vs_claim_count_avg,
        s.claim_count_min_claim_count,
        s.claim_date_tercile,
        p.end_date AS policy_end_date,
        p.status AS policy_status,
        p.product AS policy_product
    FROM with_trends s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.end_date BETWEEN DATE '2025-06-08' AND DATE '2026-09-19'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_product ORDER BY claim_date_tercile, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_2_prior,
        FIRST_VALUE(claim_count_min_claim_count) IGNORE NULLS OVER (PARTITION BY policy_product ORDER BY claim_date_tercile, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_count_min_claim_count
    FROM joined
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    status,
    claim_count_vs_claim_count_avg,
    approved_amount_vs_claim_count_avg,
    claim_date_tercile,
    policy_end_date,
    policy_status,
    policy_product,
    approved_amount_avg_2_prior,
    first_known_claim_count_min_claim_count,
    SUBSTRING(TRIM(status), 1, 4) AS status_prefix
FROM with_windows
ORDER BY status_prefix, status DESC
