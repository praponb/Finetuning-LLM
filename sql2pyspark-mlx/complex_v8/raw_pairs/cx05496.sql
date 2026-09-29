WITH cleaned_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        status,
        reported_at,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix,
        TO_DATE(reported_at) AS reported_at_date
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 9 AND 11
      AND claim_type NOT IN ('theft', 'medical')
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM cleaned_claims s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claimed_amount BETWEEN 3192.1 AND 32441.2
              AND c.policy_id <= 3
        )
      AND s.status_prefix IS NOT NULL
),
with_details AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.status_prefix,
        s.reported_at_date,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM filtered s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(claim_date) AS claim_date_year,
        policy_id,
        adjuster_team,
        COUNT(*) AS row_count,
        MAX(adjuster_level) AS max_adjuster_level,
        SUM(adjuster_level) AS total_adjuster_level,
        COUNT(DISTINCT adjuster_team) AS distinct_adjuster_teams
    FROM with_details
    GROUP BY YEAR(claim_date), policy_id, adjuster_team
    HAVING SUM(adjuster_level) > 2
)
-- Native SQL UNPIVOT (wide to long)
SELECT claim_date_year, policy_id, metric_name, reading
FROM (
    SELECT claim_date_year, policy_id, row_count, total_adjuster_level, distinct_adjuster_teams, max_adjuster_level
    FROM summary
) src
UNPIVOT (
    reading FOR metric_name IN (row_count AS m_row_count, total_adjuster_level AS total_adjuster_level_v, distinct_adjuster_teams AS distinct_adjuster_teams_v, max_adjuster_level AS m_max_adjuster_level)
) unp
