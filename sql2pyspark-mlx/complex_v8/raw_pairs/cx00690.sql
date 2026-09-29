WITH scoped AS (
    -- Scope the claims rows
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
        YEAR(reported_at) AS reported_at_year,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount,
        GREATEST(claimed_amount, approved_amount) AS greatest_claimed_amount_approved_amount
    FROM claims
    WHERE claim_date < DATE '2025-04-17'
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS last_claim_date,
        (SELECT MIN(c.claimed_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS min_claim_claimed_amount
    FROM scoped s
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        greatest_claimed_amount_approved_amount,
        last_claim_date,
        min_claim_claimed_amount,
        MIN(min_claim_claimed_amount) OVER (PARTITION BY status ORDER BY greatest_approved_amount_claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_min_claim_claimed_amount_so_far,
        MIN(min_claim_claimed_amount) OVER w_status_reported_at_month AS status_reported_at_month_min_min_claim_claimed_a,
        reported_at_year - LAG(reported_at_year) OVER (PARTITION BY status ORDER BY greatest_approved_amount_claimed_amount, id) AS reported_at_year_change,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY status, MONTH(reported_at)), 2) AS approved_amount_vs_status_reported_at_month_avg,
        RANK() OVER (PARTITION BY status ORDER BY greatest_approved_amount_claimed_amount, id) AS greatest_approved_amount_claimed_amount_rank
    FROM with_stats
    WINDOW w_status_reported_at_month AS (PARTITION BY status, MONTH(reported_at))
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM ranked s
    JOIN adjusters a ON a.id = s.adjuster_id
),
latest_per_key AS (
    -- Keep the earliest row per status, adjuster_team
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        greatest_claimed_amount_approved_amount,
        last_claim_date,
        min_claim_claimed_amount,
        min_min_claim_claimed_amount_so_far,
        status_reported_at_month_min_min_claim_claimed_a,
        reported_at_year_change,
        approved_amount_vs_status_reported_at_month_avg,
        greatest_approved_amount_claimed_amount_rank,
        adjuster_name,
        adjuster_level,
        adjuster_team
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status, adjuster_team ORDER BY status_reported_at_month_min_min_claim_claimed_a, id) AS recency_rank
        FROM with_parent
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    adjuster_id,
    claim_type,
    claimed_amount,
    greatest_claimed_amount_approved_amount,
    min_claim_claimed_amount,
    min_min_claim_claimed_amount_so_far,
    status_reported_at_month_min_min_claim_claimed_a,
    approved_amount_vs_status_reported_at_month_avg,
    greatest_approved_amount_claimed_amount_rank,
    adjuster_name,
    adjuster_team
FROM latest_per_key
ORDER BY min_min_claim_claimed_amount_so_far
