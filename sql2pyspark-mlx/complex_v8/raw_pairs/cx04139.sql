WITH scoped_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at
    FROM claims
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        reported_at,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_reported_at_to_date, 0), 4) AS claimed_amount_share_of_reported_at_to_date,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        LAST_VALUE(claimed_amount) OVER (ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        COUNT(*) OVER (PARTITION BY TO_DATE(reported_at)) AS rows_in_reported_at_to_date,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY TO_DATE(reported_at)), 2) AS claimed_amount_vs_reported_at_to_date_avg,
        MAX(claimed_amount) OVER (ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_so_far
    FROM scoped_claims
    WINDOW w_reported_at_to_date AS (PARTITION BY TO_DATE(reported_at))
),
with_parent_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.reported_at,
        s.last_known_reported_at,
        s.last_claimed_amount,
        s.rows_in_reported_at_to_date,
        s.claimed_amount_vs_reported_at_to_date_avg,
        s.max_claimed_amount_so_far,
        p.start_date AS policy_start_date,
        p.holder_id AS policy_holder_id,
        p.status AS policy_status
    FROM sequenced s
    JOIN policies p ON p.id = s.policy_id
)
-- Subtotals with ROLLUP
SELECT
    policy_id,
    COALESCE(policy_status, 'ALL') AS policy_status,
    policy_holder_id,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(policy_status)) AS policy_status_list,
    ROUND(STDDEV(claimed_amount_vs_reported_at_to_date_avg), 2) AS stddev_claimed_amount_vs_reported_at_to_date_avg,
    ROUND(100 * COUNT(CASE WHEN policy_status NOT IN ('CANCELLED', 'EXPIRED') THEN 1 END) / COUNT(*), 1) AS pct_policy_status_not_in_cancell
FROM with_parent_policy
GROUP BY ROLLUP(policy_id, policy_status, policy_holder_id)
