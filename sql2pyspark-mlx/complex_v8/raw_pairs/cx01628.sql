WITH recent AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        ABS(premium - 3521.8) AS premium_distance
    FROM policies
    WHERE status IN ('ACTIVE', 'CANCELLED')
),
qualified AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium > 2289.0
              AND p.status IN ('LAPSED', 'ACTIVE', 'CANCELLED')
        )
      AND YEAR(s.start_date) = 2025
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND YEAR(p.end_date) = 2026
        )
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.status,
        s.premium_distance,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount,
        c.adjuster_id AS claim_adjuster_id
    FROM screened s
    JOIN claims c ON c.policy_id = s.id
),
grouped AS (
    -- Group-level totals
    SELECT
        holder_id,
        claim_adjuster_id,
        status,
        COUNT(DISTINCT status) AS distinct_status,
        ROUND(100 * COUNT(CASE WHEN status IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_status_is_not_null,
        SUM(CASE WHEN status = 'EXPIRED' THEN 1 ELSE 0 END) AS n_status_expired
    FROM detail_rows_claims
    WHERE holder_id <= 17
    GROUP BY holder_id, claim_adjuster_id, status
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_all_by_pct_status_is_not_nul, 3) AS pct_status_is_not_null_cume_dist,
        NTILE(10) OVER (ORDER BY pct_status_is_not_null NULLS LAST, holder_id, claim_adjuster_id, status) AS pct_status_is_not_null_decile,
        pct_status_is_not_null - LAG(pct_status_is_not_null) OVER (ORDER BY pct_status_is_not_null NULLS LAST, holder_id, claim_adjuster_id, status) AS pct_status_is_not_null_change,
        RANK() OVER (PARTITION BY n_status_expired, status ORDER BY pct_status_is_not_null ASC) AS pct_status_is_not_null_rank,
        NTILE(5) OVER (PARTITION BY n_status_expired, status ORDER BY pct_status_is_not_null ASC, holder_id, claim_adjuster_id, status) AS pct_status_is_not_null_quintile
    FROM grouped
    WINDOW w_all_by_pct_status_is_not_nul AS (ORDER BY pct_status_is_not_null NULLS LAST, holder_id, claim_adjuster_id, status)
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.claim_adjuster_id
              AND c.claimed_amount BETWEEN 15381.6 AND 44921.3
              AND c.claim_date IS NOT NULL
        )
)
SELECT holder_id, claim_adjuster_id, field, metric_value
FROM (
    SELECT holder_id, claim_adjuster_id, pct_status_is_not_null_rank, n_status_expired, pct_status_is_not_null_quintile
    FROM without_matches
) src
UNPIVOT (
    metric_value FOR field IN (pct_status_is_not_null_rank, n_status_expired, pct_status_is_not_null_quintile)
) unp
ORDER BY holder_id, claim_adjuster_id, field
