WITH prepared AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        CONCAT_WS('|', status, claim_type) AS status_claim_type_label,
        ROUND(claimed_amount * 1.1, 2) AS claimed_amount_adj
    FROM claims
    WHERE policy_id >= 4
      AND claimed_amount <= 32878.5
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status_claim_type_label,
        claimed_amount_adj,
        MAX(claimed_amount_adj) OVER (ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_adj_so_far,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY policy_id, adjuster_id), 2) AS approved_amount_vs_policy_id_adjuster_id_avg,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY policy_id, adjuster_id)) AS distinct_status_in_policy_id_adjuster_id,
        SUM(claimed_amount_adj) OVER (ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount_adj
    FROM prepared
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND HOUR(c.reported_at) BETWEEN 6 AND 12
        )
),
latest_per_key AS (
    -- Keep the earliest row per policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status_claim_type_label,
        claimed_amount_adj,
        max_claimed_amount_adj_so_far,
        approved_amount_vs_policy_id_adjuster_id_avg,
        last_known_approved_amount,
        distinct_status_in_policy_id_adjuster_id,
        running_claimed_amount_adj
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY max_claimed_amount_adj_so_far, id) AS rn
        FROM without_matches
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_date,
    approved_amount,
    status_claim_type_label,
    claimed_amount_adj,
    max_claimed_amount_adj_so_far,
    approved_amount_vs_policy_id_adjuster_id_avg,
    distinct_status_in_policy_id_adjuster_id,
    running_claimed_amount_adj
FROM latest_per_key
ORDER BY status_claim_type_label DESC, id
LIMIT 5
