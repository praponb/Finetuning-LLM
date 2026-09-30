WITH recent AS (
    -- Keep valid rows and derive helper columns
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
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
combined_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        a.adjuster_name,
        p.end_date AS policy_end_date
    FROM recent s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(claim_date, 2) OVER (PARTITION BY adjuster_level, adjuster_id ORDER BY approved_amount DESC, id) AS next2_claim_date,
        ROW_NUMBER() OVER (PARTITION BY adjuster_level, adjuster_id ORDER BY approved_amount NULLS LAST, id) AS rn,
        LAG(adjuster_level, 3) OVER w_adjuster_team_adjuster_level AS prev3_adjuster_level,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY adjuster_level, adjuster_id ORDER BY approved_amount DESC, id)) AS days_since_prev_claim_date,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id), 2) AS adjuster_level_vs_claim_date_month_policy_id_avg,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER w_claim_date_month_policy_id, 0), 4) AS adjuster_level_share_of_claim_date_month_policy_,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_level, adjuster_id ORDER BY approved_amount DESC), 3) AS approved_amount_percent_rank,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY adjuster_level, adjuster_id ORDER BY approved_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount
    FROM combined_details_adjuster
    WINDOW w_adjuster_team_adjuster_level AS (PARTITION BY adjuster_team, adjuster_level ORDER BY approved_amount, id), w_claim_date_month_policy_id AS (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE rn = 1
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claimed_amount,
    approved_amount,
    adjuster_team,
    adjuster_level,
    next2_claim_date,
    prev3_adjuster_level,
    days_since_prev_claim_date,
    adjuster_level_vs_claim_date_month_policy_id_avg,
    approved_amount_percent_rank
FROM top_rows
WHERE days_since_prev_claim_date > 30
ORDER BY claimed_amount, adjuster_level_vs_claim_date_month_policy_id_avg
