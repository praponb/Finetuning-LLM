WITH prepared_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE status = 'APPROVED'
      AND policy_id IS NOT NULL
),
with_parent AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        a.level AS adjuster_level
    FROM prepared_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(claimed_amount) OVER (PARTITION BY adjuster_level) AS adjuster_level_max_claimed_amount,
        FIRST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY status ORDER BY reported_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_approved_amount,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_level, status ORDER BY adjuster_level DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_2_prior,
        LEAD(approved_amount) OVER (ORDER BY adjuster_level ASC, id) AS next_approved_amount,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_adjuster_level, 0), 4) AS claimed_amount_share_of_adjuster_level,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY TO_DATE(reported_at), adjuster_id ORDER BY reported_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS adjuster_level_avg_2_prior,
        LEAD(claimed_amount, 2) OVER (PARTITION BY status ORDER BY reported_at NULLS LAST, id) AS next2_claimed_amount,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        DENSE_RANK() OVER w_policy_id_by_approved_amount_2 AS approved_amount_dense_rank
    FROM with_parent
    WINDOW w_adjuster_level AS (PARTITION BY adjuster_level), w_policy_id_by_approved_amount_2 AS (PARTITION BY policy_id ORDER BY approved_amount, id)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at,
        adjuster_level,
        first_known_approved_amount,
        next_approved_amount,
        claimed_amount_share_of_adjuster_level,
        adjuster_level_avg_2_prior,
        next2_claimed_amount,
        last_known_approved_amount,
        LENGTH(TRIM(status)) AS status_len,
        ABS(next_approved_amount - 4554.8) AS next_approved_amount_distance,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix
    FROM ranked
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(approved_amount) OVER w_status_len_status AS status_len_status_max_approved_amount,
        ROUND(AVG(status_len) OVER (PARTITION BY status_len, status), 2) AS status_len_status_avg_status_len,
        LEAD(status_len, 3) OVER (PARTITION BY adjuster_id, status ORDER BY status_len, id) AS next3_status_len,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status_len, adjuster_id ORDER BY next_approved_amount_distance, id), 3) AS next_approved_amount_distance_percent_rank
    FROM enriched
    WINDOW w_status_len_status AS (PARTITION BY status_len, status)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        adjuster_id,
        adjuster_level,
        SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END) AS n_status_rejected,
        COUNT(*) AS row_count
    FROM windowed
    WHERE claimed_amount_share_of_adjuster_level >= 0.2
    GROUP BY adjuster_id, adjuster_level
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(row_count, 1, 0) OVER (ORDER BY row_count ASC, adjuster_id, adjuster_level) AS prev_row_count,
        ROUND(AVG(adjuster_level) OVER (ORDER BY row_count ASC, adjuster_id, adjuster_level ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS adjuster_level_avg_6_prior
    FROM rollup_base
)
-- Final projection
SELECT
    adjuster_id,
    adjuster_level,
    row_count,
    prev_row_count,
    adjuster_level_avg_6_prior,
    ABS(adjuster_level - 3) AS adjuster_level_distance
FROM sequenced
