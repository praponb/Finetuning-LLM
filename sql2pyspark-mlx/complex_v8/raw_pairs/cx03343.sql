WITH prepared AS (
    -- Scope the claims rows
    SELECT *
    FROM claims
    WHERE policy_id <= 9
      AND status = 'REJECTED'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY MONTH(reported_at), adjuster_id) AS rows_in_reported_at_month_adjuster_id,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        ROUND(CUME_DIST() OVER (ORDER BY reported_at, id), 3) AS reported_at_cume_dist
    FROM prepared
),
enriched_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.rows_in_reported_at_month_adjuster_id,
        s.first_known_claim_date,
        s.reported_at_cume_dist,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM ranked s
    JOIN adjusters a ON a.id = s.adjuster_id
),
ranked_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        rows_in_reported_at_month_adjuster_id,
        first_known_claim_date,
        reported_at_cume_dist,
        adjuster_team,
        SIZE(COLLECT_SET(adjuster_team) OVER w_first_known_claim_date_month) AS distinct_adjuster_team_in_first_known_claim_date,
        MAX(reported_at_cume_dist) OVER w_first_known_claim_date_month AS first_known_claim_date_month_claim_type_max_repo
    FROM enriched_adjuster
    WINDOW w_first_known_claim_date_month AS (PARTITION BY DATE_TRUNC('MONTH', first_known_claim_date), claim_type)
)
SELECT
    COALESCE(adjuster_team, 'ALL') AS adjuster_team,
    distinct_adjuster_team_in_first_known_claim_date,
    COALESCE(status, 'ALL') AS status,
    SUM(CASE WHEN status IN ('APPROVED', 'PAID') THEN distinct_adjuster_team_in_first_known_claim_date ELSE 0 END) AS distinct_adjuster_team_in_first_known_claim_date_2,
    COUNT(*) AS cnt,
    COUNT(DISTINCT claim_type) AS distinct_claim_types
FROM ranked_2
GROUP BY CUBE(adjuster_team, distinct_adjuster_team_in_first_known_claim_date, status)
