WITH valid AS (
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
        YEAR(reported_at) AS reported_at_year,
        claimed_amount + approved_amount AS claimed_amount_plus_approved_amount,
        CONCAT_WS('|', status, claim_type) AS status_claim_type_label
    FROM claims
    WHERE adjuster_id <= 4
      AND claim_type = 'delay'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_adjuster_id AS rows_in_adjuster_id,
        SUM(approved_amount) OVER (PARTITION BY policy_id, adjuster_id ORDER BY claim_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS approved_amount_sum_3,
        SUM(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at), claim_type ORDER BY claimed_amount_plus_approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_approved_amount,
        ROUND(CUME_DIST() OVER w_policy_id_adjuster_id_by_cla_2, 3) AS claim_date_cume_dist,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_adjuster_id, 0), 4) AS approved_amount_share_of_adjuster_id,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY adjuster_id)) AS distinct_status_in_adjuster_id
    FROM valid
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id), w_policy_id_adjuster_id_by_cla_2 AS (PARTITION BY policy_id, adjuster_id ORDER BY claim_date DESC, id)
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claim_type NOT IN ('collision', 'flood')
              AND c.claim_date IS NOT NULL
        )
      AND s.claimed_amount < 38909.4
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM with_activity
    WHERE rows_in_adjuster_id < (SELECT MAX(rows_in_adjuster_id) FROM with_activity)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    approved_amount,
    status,
    reported_at,
    reported_at_year,
    claimed_amount_plus_approved_amount,
    status_claim_type_label,
    rows_in_adjuster_id,
    approved_amount_sum_3,
    running_approved_amount,
    claim_date_cume_dist,
    LEAST(approved_amount, rows_in_adjuster_id) AS least_approved_amount_rows_in_adjuster_id,
    DATE_TRUNC('MONTH', claim_date) AS claim_date_month
FROM without_matches
