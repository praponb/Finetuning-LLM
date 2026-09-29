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
        reported_at
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 9 AND 14
),
with_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.reported_at,
        p.status AS policy_status
    FROM cleaned_claims s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.status <> 'LAPSED'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(claim_date, 2) OVER (PARTITION BY MONTH(claim_date), claim_type ORDER BY claim_date ASC, id) AS prev2_claim_date,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(reported_at) ORDER BY reported_at, id), 3) AS reported_at_cume_dist,
        FIRST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY MONTH(reported_at) ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reported_at,
        RANK() OVER w_claim_date_month_claim_type_2 AS claim_date_rank
    FROM with_details_policy
    WINDOW w_claim_date_month_claim_type_2 AS (PARTITION BY MONTH(claim_date), claim_type ORDER BY claim_date ASC)
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM metrics
    WHERE claim_date_rank <= 2
)
SELECT
    policy_id,
    claim_date_rank,
    ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
    ROUND(SUM(claim_date_rank) / NULLIF(SUM(reported_at_cume_dist), 0), 3) AS claim_date_rank_to_reported_at_cume_dist_ratio,
    MIN(claim_date_rank) AS min_claim_date_rank
FROM first_rows
GROUP BY GROUPING SETS ((policy_id, claim_date_rank), (policy_id), (claim_date_rank))
