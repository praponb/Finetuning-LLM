WITH base_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND policy_id IS NOT NULL
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.reported_at,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_type AS claim_claim_type
    FROM base_claims s
    JOIN claims c ON c.policy_id = s.policy_id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month,
        COUNT(*) AS row_count,
        MAX(reported_at) AS last_reported_at
    FROM line_level_claims
    GROUP BY DATE_TRUNC('MONTH', reported_at)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        reported_at_month,
        row_count,
        ROW_NUMBER() OVER (PARTITION BY row_count ORDER BY last_reported_at, reported_at_month) AS last_reported_at_seq,
        LAST_VALUE(last_reported_at) OVER (PARTITION BY row_count ORDER BY row_count DESC, reported_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_reported_at,
        LEAD(row_count) OVER (PARTITION BY row_count ORDER BY last_reported_at, reported_at_month) AS next_row_count,
        FIRST_VALUE(row_count) IGNORE NULLS OVER (PARTITION BY row_count ORDER BY last_reported_at, reported_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_row_count
    FROM rollup_base
)
SELECT
    reported_at_month,
    row_count,
    last_reported_at_seq,
    next_row_count,
    first_known_row_count,
    row_count + first_known_row_count AS row_count_plus_first_known_row_count,
    ROUND((UNIX_TIMESTAMP(last_last_reported_at) - UNIX_TIMESTAMP(reported_at_month)) / 3600, 1) AS hours_reported_at_month_to_last_last_reported_at
FROM with_windows
ORDER BY first_known_row_count
