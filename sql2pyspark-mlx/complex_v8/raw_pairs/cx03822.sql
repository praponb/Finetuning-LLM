WITH prepared_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        ROUND(claimed_amount * 1.07, 2) AS claimed_amount_adj,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        ROUND(claimed_amount * 1.5, 2) AS claimed_amount_adj_2
    FROM claims
    WHERE policy_id <= 22
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count,
        (SELECT COUNT(CASE WHEN c.claim_type IS NOT NULL THEN 1 END) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_claim_type_count,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_reported_at
    FROM prepared_claims s
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        claimed_amount_adj,
        claimed_amount_bucket,
        claimed_amount_adj_2,
        claim_count,
        claim_claim_type_count,
        last_claim_reported_at,
        ROUND(CUME_DIST() OVER (ORDER BY claim_claim_type_count NULLS LAST), 3) AS claim_claim_type_count_cume_dist,
        NTH_VALUE(claim_count, 2) OVER (ORDER BY claim_claim_type_count NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_count,
        NTILE(3) OVER w_claim_date_year_claim_type_b AS claim_date_tercile,
        SUM(claimed_amount_adj) OVER (ORDER BY claim_claim_type_count NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount_adj,
        NTILE(5) OVER w_claim_claim_type_count_by_cl AS claimed_amount_quintile
    FROM profiled
    WINDOW w_claim_date_year_claim_type_b AS (PARTITION BY YEAR(claim_date), claim_type ORDER BY claim_date DESC, id), w_claim_claim_type_count_by_cl AS (PARTITION BY claim_claim_type_count ORDER BY claimed_amount DESC, id)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY claim_claim_type_count ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        ROUND(CUME_DIST() OVER w_all_by_approved_amount, 3) AS approved_amount_cume_dist,
        ROUND(AVG(claim_date_tercile) OVER (PARTITION BY claim_claim_type_count ORDER BY approved_amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS claim_date_tercile_avg_3,
        FIRST_VALUE(second_claim_count) IGNORE NULLS OVER (PARTITION BY claim_date_tercile, policy_id ORDER BY claim_count ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_second_claim_count
    FROM with_trends
    WINDOW w_all_by_approved_amount AS (ORDER BY approved_amount, id)
)
-- Final projection
SELECT
    id,
    approved_amount,
    status,
    claimed_amount_bucket,
    claimed_amount_adj_2,
    claim_count,
    claim_claim_type_count,
    last_claim_reported_at,
    second_claim_count,
    claim_date_tercile,
    claimed_amount_quintile,
    last_known_approved_amount
FROM ranked
WHERE claimed_amount_adj_2 <= 63765.0
ORDER BY id DESC, last_claim_reported_at
