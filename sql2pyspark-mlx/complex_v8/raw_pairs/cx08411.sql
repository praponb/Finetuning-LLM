WITH recent_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        DATE_TRUNC('WEEK', claim_date) AS claim_date_week
    FROM claims
    WHERE claim_date BETWEEN DATE '2024-04-11' AND DATE '2025-08-25'
      AND adjuster_id <= 4
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.claim_date_week,
        c.id AS claim_id,
        c.claim_date,
        c.reported_at AS claim_reported_at
    FROM recent_claims s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.claim_date < DATE '2024-08-20'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(claim_date_week, 2) OVER (PARTITION BY status, adjuster_id ORDER BY claim_date NULLS LAST, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date_week,
        RANK() OVER (PARTITION BY status, adjuster_id ORDER BY claim_date NULLS LAST) AS claim_date_rank,
        NTILE(4) OVER w_claim_date_month_policy_id_b AS approved_amount_quartile,
        MAX(approved_amount) OVER w_status AS status_max_approved_amount,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id ORDER BY approved_amount, id, claim_id) AS seq_no,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id ORDER BY approved_amount, id, claim_id) AS approved_amount_rank
    FROM detail_rows
    WINDOW w_claim_date_month_policy_id_b AS (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id ORDER BY approved_amount, id, claim_id), w_status AS (PARTITION BY status)
),
enriched AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.claim_id,
        s.claim_date,
        s.second_claim_date_week,
        s.approved_amount_quartile,
        s.status_max_approved_amount,
        s.seq_no,
        p.end_date AS policy_end_date,
        p.start_date AS policy_start_date
    FROM windowed s
    LEFT JOIN policies p ON p.id = s.policy_id
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.claim_id,
        s.claim_date,
        s.second_claim_date_week,
        s.approved_amount_quartile,
        s.status_max_approved_amount,
        s.policy_start_date,
        c.id AS claim_id_2,
        c.approved_amount AS claim_approved_amount,
        c.claim_date AS claim_claim_date,
        c.adjuster_id AS claim_adjuster_id
    FROM enriched s
    JOIN claims c ON c.policy_id = s.policy_id
),
adjuster_id_stats AS (
    -- Max status_max_approved_amount per adjuster_id
    SELECT adjuster_id, MAX(status_max_approved_amount) AS max_status_max_approved_amount
    FROM line_level_claims
    GROUP BY adjuster_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_status_max_approved_amount,
        ROUND(s.status_max_approved_amount - g.max_status_max_approved_amount, 2) AS status_max_approved_amount_minus_max
    FROM line_level_claims s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.status_max_approved_amount = g.max_status_max_approved_amount
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, MAX(reported_at) AS last_claim_reported_at
    FROM claims
    GROUP BY adjuster_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_reported_at
    FROM outliers s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount,
    approved_amount,
    claim_id,
    second_claim_date_week,
    approved_amount_quartile,
    status_max_approved_amount,
    claim_id_2,
    claim_approved_amount,
    claim_claim_date,
    claim_adjuster_id,
    max_status_max_approved_amount,
    status_max_approved_amount_minus_max
FROM profiled
ORDER BY status_max_approved_amount_minus_max DESC NULLS FIRST
