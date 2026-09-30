WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        LENGTH(TRIM(claim_type)) AS claim_type_len
    FROM claims
    WHERE approved_amount <= 21750.2
      AND claim_type <> 'flood'
),
adjuster_id_stats AS (
    -- Avg claimed_amount per adjuster_id
    SELECT adjuster_id, AVG(claimed_amount) AS avg_claimed_amount
    FROM prepared
    GROUP BY adjuster_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claimed_amount,
        ROUND(s.claimed_amount - g.avg_claimed_amount, 2) AS claimed_amount_minus_avg
    FROM prepared s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.claimed_amount > g.avg_claimed_amount
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_date,
        (SELECT COUNT(CASE WHEN c.claim_type IN ('theft', 'collision', 'flood') THEN 1 END) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_claim_type_count
    FROM vs_group s
)
SELECT
    claim_claim_type_count,
    claim_type_len,
    policy_id,
    GROUPING(claim_claim_type_count) AS is_total_claim_claim_type_count,
    ROUND(STDDEV(approved_amount), 2) AS stddev_approved_amount,
    MIN(reported_at) AS first_reported_at,
    ROUND(STDDEV(avg_claimed_amount), 2) AS stddev_avg_claimed_amount
FROM summarized_rows
GROUP BY CUBE(claim_claim_type_count, claim_type_len, policy_id)
ORDER BY claim_claim_type_count NULLS LAST, claim_type_len NULLS LAST, policy_id NULLS LAST
