WITH recent_claims AS (
    -- Filter claims
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
        CONCAT_WS(': ', status, claim_type) AS status_claim_type_label,
        DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label
    FROM claims
    WHERE YEAR(claim_date) = 2024
      AND status IN ('IN_REVIEW', 'APPROVED', 'OPEN')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(claimed_amount) OVER (PARTITION BY claim_type) AS claim_type_min_claimed_amount,
        DENSE_RANK() OVER w_adjuster_id_claim_type_by_re AS reported_at_dense_rank,
        ROUND(CUME_DIST() OVER (ORDER BY claimed_amount ASC, id), 3) AS claimed_amount_cume_dist,
        SUM(claimed_amount) OVER (ORDER BY claimed_amount ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS claimed_amount_sum_2_prior
    FROM recent_claims
    WINDOW w_adjuster_id_claim_type_by_re AS (PARTITION BY adjuster_id, claim_type ORDER BY reported_at DESC, id)
),
with_parent_adjuster AS (
    -- Join adjusters
    SELECT
        s.*,
        a.level AS adjuster_level,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM ranked s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT
    COALESCE(claim_type, 'ALL') AS claim_type,
    adjuster_id,
    ROUND(STDDEV(reported_at_dense_rank), 2) AS stddev_reported_at_dense_rank,
    COUNT(*) AS row_count
FROM with_parent_adjuster
GROUP BY GROUPING SETS ((claim_type, adjuster_id), (claim_type), ())
ORDER BY claim_type NULLS LAST, adjuster_id NULLS LAST
