WITH base AS (
    -- Keep valid rows and derive helper columns
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
),
with_parent_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.reported_at,
        p.holder_id AS policy_holder_id,
        p.start_date AS policy_start_date
    FROM base s
    JOIN policies p ON p.id = s.policy_id
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_parent_policy s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY policy_id, claim_type ORDER BY reported_at, id) AS seq_no,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id)) AS distinct_policy_id_in_adjuster_id,
        ROW_NUMBER() OVER w_all_by_reported_at AS reported_at_seq,
        ROW_NUMBER() OVER w_claim_type_adjuster_id_by_re AS reported_at_seq_2,
        ROUND(CUME_DIST() OVER w_claim_type_adjuster_id_by_re, 3) AS reported_at_cume_dist,
        MAX(claimed_amount) OVER (PARTITION BY adjuster_id) AS adjuster_id_max_claimed_amount
    FROM with_activity
    WINDOW w_all_by_reported_at AS (ORDER BY reported_at DESC, id), w_claim_type_adjuster_id_by_re AS (PARTITION BY claim_type, adjuster_id ORDER BY reported_at, id)
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_seq_no AS rows_in_seq_no,
        SUM(reported_at_seq) OVER (PARTITION BY seq_no) AS seq_no_sum_reported_at_seq,
        LAST_VALUE(seq_no) IGNORE NULLS OVER (PARTITION BY seq_no, reported_at_seq ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_seq_no,
        MAX(reported_at_cume_dist) OVER (PARTITION BY seq_no) AS seq_no_max_reported_at_cume_dist,
        reported_at_seq - LAG(reported_at_seq) OVER (PARTITION BY seq_no, reported_at_seq ORDER BY reported_at DESC, id) AS reported_at_seq_change
    FROM metrics
    WINDOW w_seq_no AS (PARTITION BY seq_no)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    reported_at,
    policy_start_date,
    distinct_policy_id_in_adjuster_id,
    reported_at_seq_2,
    reported_at_cume_dist,
    adjuster_id_max_claimed_amount,
    rows_in_seq_no,
    reported_at_seq_change
FROM metrics_2
WHERE reported_at_cume_dist <= 0.8
ORDER BY reported_at_seq_change DESC, reported_at_cume_dist DESC, id
LIMIT 5
