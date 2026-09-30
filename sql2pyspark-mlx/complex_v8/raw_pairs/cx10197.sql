WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        reported_at + INTERVAL 1 DAYS AS reported_at_plus_1d
    FROM claims
    WHERE claimed_amount <= 34763.4
      AND policy_id <= 28
),
joined_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.status_or_unknown,
        a.adjuster_name
    FROM prepared s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
status_stats AS (
    -- Avg claimed_amount per status
    SELECT status, AVG(claimed_amount) AS avg_claimed_amount
    FROM joined_adjuster
    GROUP BY status
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claimed_amount,
        ROUND(s.claimed_amount - g.avg_claimed_amount, 2) AS claimed_amount_minus_avg
    FROM joined_adjuster s
    JOIN status_stats g ON g.status = s.status
    WHERE s.claimed_amount > g.avg_claimed_amount
),
unique_rows AS (
    -- Keep the latest row per policy_id, adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        status_or_unknown,
        adjuster_name,
        avg_claimed_amount,
        claimed_amount_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id, adjuster_id ORDER BY claimed_amount_minus_avg DESC, id DESC) AS recency_rank
        FROM compared
    ) ranked
    WHERE recency_rank = 1
)
SELECT DISTINCT policy_id, claim_date, claimed_amount_minus_avg
FROM unique_rows
ORDER BY claim_date DESC, policy_id
