WITH cleaned_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        CASE WHEN approved_amount >= 14595.6 THEN 'A' WHEN approved_amount >= 492.6 THEN 'B' ELSE 'C' END AS approved_amount_tier,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref
    FROM claims
    WHERE status IN ('IN_REVIEW', 'OPEN', 'PAID')
      AND approved_amount < 16404.6
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(claim_date) OVER (ORDER BY approved_amount DESC NULLS FIRST, id) AS next_claim_date,
        ROUND(PERCENT_RANK() OVER (ORDER BY approved_amount DESC NULLS FIRST, id), 3) AS approved_amount_percent_rank,
        LAST_VALUE(claim_date) OVER (ORDER BY approved_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (ORDER BY approved_amount DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_reported_at,
        SUM(approved_amount) OVER (PARTITION BY status_or_unknown) AS status_or_unknown_sum_approved_amount,
        SIZE(COLLECT_SET(policy_id) OVER w_status_or_unknown) AS distinct_policy_id_in_status_or_unknown
    FROM cleaned_claims
    WINDOW w_status_or_unknown AS (PARTITION BY status_or_unknown)
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM ranked s
    WHERE s.adjuster_id IN (
            SELECT c.adjuster_id
            FROM claims c
            WHERE c.claim_date >= DATE '2024-06-07'
        )
),
without_matches_2 AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM without_matches s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.status IN ('APPROVED', 'PAID', 'OPEN')
              AND c.claimed_amount <= 44991.5
        )
      AND YEAR(s.last_claim_date) = 2025
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        claim_type,
        MAX(status_or_unknown_sum_approved_amount) AS max_status_or_unknown_sum_approved_amount,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        COUNT(DISTINCT claim_type) AS distinct_claim_types
    FROM without_matches_2
    GROUP BY claim_type
)
SELECT *
FROM (
    SELECT distinct_policy_ids, distinct_claim_types, claim_type, max_status_or_unknown_sum_approved_amount
    FROM rollup_base
) src
PIVOT (
    MAX(max_status_or_unknown_sum_approved_amount)
    FOR claim_type IN ('theft', 'delay')
)
ORDER BY distinct_policy_ids, distinct_claim_types
