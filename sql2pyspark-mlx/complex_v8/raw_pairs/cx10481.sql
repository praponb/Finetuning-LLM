WITH base_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount
    FROM claims
    WHERE status IN ('REJECTED', 'APPROVED')
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM base_claims
    WHERE claimed_amount < (SELECT MAX(claimed_amount) FROM base_claims)
),
joined_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status_or_unknown,
        s.claimed_amount_per_approved_amount,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM filtered s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM joined_adjuster s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.approved_amount BETWEEN 0.0 AND 17759.3
        )
),
policy_id_stats AS (
    -- Avg approved_amount per policy_id
    SELECT policy_id, AVG(approved_amount) AS avg_approved_amount
    FROM without_matches
    GROUP BY policy_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_approved_amount,
        ROUND(s.approved_amount - g.avg_approved_amount, 2) AS approved_amount_minus_avg
    FROM without_matches s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.approved_amount > g.avg_approved_amount
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        status_or_unknown,
        claim_type,
        policy_id,
        PERCENTILE_APPROX(avg_approved_amount, 0.5) AS median_avg_approved_amount,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        ARRAY_SORT(COLLECT_SET(adjuster_team)) AS adjuster_team_list,
        SUM(approved_amount) AS total_approved_amount
    FROM above_average
    GROUP BY status_or_unknown, claim_type, policy_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        status_or_unknown,
        claim_type,
        policy_id,
        claim_type_list,
        adjuster_team_list,
        total_approved_amount,
        SUM(median_avg_approved_amount) OVER (PARTITION BY claim_type ORDER BY total_approved_amount, status_or_unknown, claim_type, policy_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS median_avg_approved_amount_sum_2,
        ROUND(AVG(median_avg_approved_amount) OVER (PARTITION BY policy_id), 2) AS policy_id_avg_median_avg_approved_amount,
        LAST_VALUE(median_avg_approved_amount) IGNORE NULLS OVER (PARTITION BY claim_type ORDER BY total_approved_amount, status_or_unknown, claim_type, policy_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_median_avg_approved_amount,
        FIRST_VALUE(median_avg_approved_amount) IGNORE NULLS OVER (PARTITION BY claim_type ORDER BY total_approved_amount, status_or_unknown, claim_type, policy_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_median_avg_approved_amount
    FROM rollup_base
)
SELECT
    status_or_unknown,
    claim_type,
    policy_id,
    claim_type_list,
    adjuster_team_list,
    total_approved_amount,
    median_avg_approved_amount_sum_2,
    policy_id_avg_median_avg_approved_amount,
    last_known_median_avg_approved_amount
FROM sequenced
ORDER BY median_avg_approved_amount_sum_2
