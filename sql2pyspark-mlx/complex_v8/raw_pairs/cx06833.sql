WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        CASE WHEN approved_amount >= 14668.0 THEN 'A' WHEN approved_amount >= 4554.8 THEN 'B' ELSE 'C' END AS approved_amount_tier
    FROM claims
    WHERE claim_date >= DATE '2024-04-27'
      AND reported_at < TIMESTAMP '2025-02-18 09:00:00'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        approved_amount_tier,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id, status)) AS distinct_policy_id_in_adjuster_id_status,
        ROUND(AVG(approved_amount) OVER (PARTITION BY adjuster_id, status), 2) AS adjuster_id_status_avg_approved_amount
    FROM filtered
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM ranked s
    WHERE s.adjuster_id IN (
            SELECT c.adjuster_id
            FROM claims c
        )
      AND s.status IS NOT NULL
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM screened
    WHERE distinct_policy_id_in_adjuster_id_status > (SELECT AVG(distinct_policy_id_in_adjuster_id_status) FROM screened)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        adjuster_id,
        approved_amount,
        approved_amount_tier,
        distinct_policy_id_in_adjuster_id_status,
        COALESCE(approved_amount_tier, 'UNKNOWN') AS approved_amount_tier_or_unknown,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount,
        CONCAT_WS(': ', approved_amount_tier, status) AS approved_amount_tier_status_label
    FROM without_matches
    WHERE distinct_policy_id_in_adjuster_id_status >= 3
)
SELECT id, measure, value
FROM (
    SELECT id, approved_amount_per_claimed_amount, approved_amount
    FROM tagged
) src
UNPIVOT (
    value FOR measure IN (approved_amount_per_claimed_amount AS m_approved_amount_per_claimed_amount, approved_amount AS approved_amount_v)
) unp
