WITH valid AS (
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
        reported_at,
        UPPER(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE claim_type NOT IN ('delay', 'fire')
      AND approved_amount >= 2349.3
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY claim_date ASC, id) AS claim_date_dense_rank,
        NTILE(3) OVER w_claim_date_month_by_claimed AS claimed_amount_tercile,
        LEAD(reported_at) OVER (PARTITION BY MONTH(claim_date) ORDER BY claimed_amount DESC, id) AS next_reported_at,
        FIRST_VALUE(approved_amount) OVER (ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount,
        DENSE_RANK() OVER (PARTITION BY MONTH(claim_date) ORDER BY claimed_amount DESC, id) AS claimed_amount_dense_rank
    FROM valid
    WINDOW w_claim_date_month_by_claimed AS (PARTITION BY MONTH(claim_date) ORDER BY claimed_amount DESC, id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(claimed_amount_tercile / NULLIF(approved_amount, 0), 3) AS claimed_amount_tercile_per_approved_amount
    FROM ranked
    WHERE approved_amount < 24718.5
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.reported_at,
        s.claim_date_dense_rank,
        s.first_approved_amount,
        s.claimed_amount_tercile_per_approved_amount,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.claimed_amount AS claim_claimed_amount
    FROM tagged s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.claimed_amount BETWEEN 13898.1 AND 43724.9
)
SELECT
    claim_policy_id,
    adjuster_id,
    policy_id,
    PERCENTILE_APPROX(claimed_amount_tercile_per_approved_amount, 0.5) AS median_claimed_amount_tercile_per_approved_amoun,
    ROUND(STDDEV(first_approved_amount), 2) AS stddev_first_approved_amount
FROM line_level
GROUP BY ROLLUP(claim_policy_id, adjuster_id, policy_id)
