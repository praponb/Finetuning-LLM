WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at + INTERVAL 2 HOURS AS reported_at_plus_2h,
        ABS(approved_amount - 17759.3) AS approved_amount_distance,
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month
    FROM claims
    WHERE approved_amount > 6817.6
      AND reported_at >= TIMESTAMP '2025-01-31 13:00:00'
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count
    FROM cleaned_claims s
)
SELECT
    adjuster_id,
    claim_count,
    MAX(reported_at_plus_2h) AS last_reported_at_plus_2h,
    COUNT(*) FILTER (WHERE claim_type = 'fire') AS n_claim_type_fire
FROM summarized_rows
GROUP BY GROUPING SETS ((adjuster_id, claim_count), (adjuster_id), ())
ORDER BY adjuster_id NULLS LAST, claim_count NULLS LAST
