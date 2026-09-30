WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE policy_id <= 8
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        status,
        MIN(approved_amount) OVER (PARTITION BY status, policy_id) AS status_policy_id_min_approved_amount,
        FIRST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY status ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reported_at,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at), status ORDER BY approved_amount DESC, id), 3) AS approved_amount_cume_dist
    FROM filtered_claims
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY adjuster_id ORDER BY first_known_reported_at DESC, id) AS first_known_reported_at_dense_rank,
        NTH_VALUE(approved_amount_cume_dist, 2) OVER (PARTITION BY adjuster_id ORDER BY first_known_reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount_cume_dist
    FROM windowed
)
SELECT
    status,
    adjuster_id,
    first_known_reported_at_dense_rank,
    GROUPING(status) AS is_total_status,
    DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days,
    COUNT(*) AS cnt
FROM sequenced
GROUP BY ROLLUP(status, adjuster_id, first_known_reported_at_dense_rank)
ORDER BY status NULLS LAST, adjuster_id NULLS LAST, first_known_reported_at_dense_rank NULLS LAST
