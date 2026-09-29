WITH cleaned_claims AS (
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
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date
    FROM claims
    WHERE policy_id <= 4
      AND approved_amount BETWEEN 0.0 AND 23207.7
),
with_details AS (
    -- Join adjusters
    SELECT
        s.*,
        a.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM cleaned_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(reported_at) OVER (PARTITION BY policy_id, claim_type ORDER BY reported_at, id) AS next_reported_at,
        COUNT(*) OVER w_adjuster_team AS rows_in_adjuster_team,
        ROUND(days_since_claim_date / NULLIF(SUM(days_since_claim_date) OVER w_adjuster_team, 0), 4) AS days_since_claim_date_share_of_adjuster_team,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY adjuster_team)) AS distinct_adjuster_id_in_adjuster_team,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER (PARTITION BY adjuster_team, status), 0), 4) AS adjuster_level_share_of_adjuster_team_status,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY adjuster_level, DATE_TRUNC('MONTH', reported_at) ORDER BY approved_amount, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS adjuster_level_avg_2,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY days_since_claim_date DESC, id) AS days_since_claim_date_dense_rank,
        FIRST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY adjuster_level ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_approved_amount,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_id, claim_type ORDER BY reported_at, id), 3) AS reported_at_percent_rank,
        SIZE(COLLECT_SET(claim_type) OVER w_adjuster_team) AS distinct_claim_type_in_adjuster_team,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY adjuster_team), 0), 4) AS approved_amount_share_of_adjuster_team,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_level, DATE_TRUNC('MONTH', reported_at) ORDER BY approved_amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_4
    FROM with_details
    WINDOW w_adjuster_team AS (PARTITION BY adjuster_team)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    days_since_claim_date,
    adjuster_name,
    adjuster_team,
    rows_in_adjuster_team,
    distinct_adjuster_id_in_adjuster_team,
    days_since_claim_date_dense_rank,
    first_known_approved_amount,
    reported_at_percent_rank,
    distinct_claim_type_in_adjuster_team,
    approved_amount_share_of_adjuster_team,
    claimed_amount_avg_4,
    GREATEST(adjuster_level_avg_2, adjuster_level) AS greatest_adjuster_level_avg_2_adjuster_level,
    TO_DATE(next_reported_at) AS next_reported_at_date
FROM with_windows
WHERE reported_at_percent_rank >= 0.2
ORDER BY next_reported_at_date, id
LIMIT 20
