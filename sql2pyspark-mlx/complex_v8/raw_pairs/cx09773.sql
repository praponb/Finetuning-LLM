WITH recent_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        MONTH(claim_date) AS claim_date_month,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount
    FROM claims
    WHERE claim_date < DATE '2025-04-25'
),
status_stats AS (
    -- Avg claim_date_month per status
    SELECT status, AVG(claim_date_month) AS avg_claim_date_month
    FROM recent_claims
    GROUP BY status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claim_date_month,
        ROUND(s.claim_date_month - g.avg_claim_date_month, 2) AS claim_date_month_minus_avg
    FROM recent_claims s
    JOIN status_stats g ON g.status = s.status
    WHERE s.claim_date_month > g.avg_claim_date_month
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        claimed_amount_per_approved_amount,
        CONCAT_WS('|', status, claim_type) AS status_claim_type_label,
        ROUND(avg_claim_date_month * 1.5, 2) AS avg_claim_date_month_adj
    FROM outliers
    WHERE reported_at < TIMESTAMP '2025-03-02 12:00:00'
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.policy_id = s.policy_id) AS first_claim_reported_at
    FROM scored s
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.claimed_amount_per_approved_amount,
        s.avg_claim_date_month_adj,
        s.first_claim_reported_at,
        p.status AS policy_status,
        a.team AS adjuster_team
    FROM summarized_rows s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claimed_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS min_claim_claimed_amount,
        (SELECT MIN(c.claimed_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS min_claim_claimed_amount_2
    FROM combined_details s
),
combined_details_policy AS (
    -- Enrich with dimension data
    SELECT s.*, p.holder_id AS policy_holder_id, a.adjuster_name
    FROM with_counts s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT DISTINCT avg_claim_date_month_adj, claim_type, min_claim_claimed_amount
FROM combined_details_policy
WHERE claim_type IN ('flood', 'delay', 'medical')
