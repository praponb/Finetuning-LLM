WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 7 AND 10
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM filtered_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.adjuster_id IS NOT NULL
              AND c.status NOT IN ('PAID', 'OPEN')
        )
      AND s.adjuster_id >= 5
),
with_details_adjuster AS (
    -- Join adjusters
    SELECT s.*, a.team AS adjuster_team
    FROM filtered s
    JOIN adjusters a ON a.id = s.adjuster_id AND LOWER(a.adjuster_name) LIKE '%a %'
    WHERE a.team IS NOT NULL
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        adjuster_team,
        MAX(approved_amount) OVER (PARTITION BY adjuster_id ORDER BY approved_amount DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS approved_amount_max_2,
        MAX(approved_amount) OVER (PARTITION BY adjuster_id ORDER BY approved_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_approved_amount_so_far
    FROM with_details_adjuster
),
without_matches AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.level >= 1
              AND UPPER(TRIM(a.adjuster_name)) LIKE 'F%'
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, reading
FROM (
    SELECT id, claimed_amount, approved_amount, max_approved_amount_so_far
    FROM without_matches
) src
UNPIVOT (
    reading FOR metric IN (claimed_amount, approved_amount, max_approved_amount_so_far)
) unp
