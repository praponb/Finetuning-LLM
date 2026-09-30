WITH prepared_claims AS (
    -- Filter claims
    SELECT *
    FROM claims
    WHERE claimed_amount > 18778.6
),
enriched AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        p.end_date AS policy_end_date
    FROM prepared_claims s
    JOIN policies p ON p.id = s.policy_id
    WHERE YEAR(p.end_date) = 2026
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        policy_end_date,
        MAX(claimed_amount) OVER (PARTITION BY status) AS status_max_claimed_amount,
        SUM(approved_amount) OVER w_status AS status_sum_approved_amount,
        ROUND(AVG(approved_amount) OVER w_status, 2) AS status_avg_approved_amount,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY status), 0), 4) AS approved_amount_share_of_status,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_status_claim_date_month_by_p) AS days_since_prev_claim_date,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY claim_type, policy_id ORDER BY claim_date DESC, id)) AS days_since_prev_claim_date_2,
        MIN(approved_amount) OVER (PARTITION BY status) AS status_min_approved_amount,
        FIRST_VALUE(claimed_amount) OVER (PARTITION BY claim_type, policy_id ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount
    FROM enriched
    WINDOW w_status AS (PARTITION BY status), w_status_claim_date_month_by_p AS (PARTITION BY status, DATE_TRUNC('MONTH', claim_date) ORDER BY policy_end_date, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        approved_amount,
        policy_end_date,
        status_max_claimed_amount,
        status_sum_approved_amount,
        status_avg_approved_amount,
        days_since_prev_claim_date_2,
        status_min_approved_amount,
        LEAD(approved_amount) OVER w_all_by_claim_date AS next_approved_amount,
        ROUND(days_since_prev_claim_date - AVG(days_since_prev_claim_date) OVER (PARTITION BY adjuster_id), 2) AS days_since_prev_claim_date_vs_adjuster_id_avg,
        ROW_NUMBER() OVER w_all_by_reported_at AS reported_at_seq,
        ROUND(CUME_DIST() OVER (ORDER BY reported_at), 3) AS reported_at_cume_dist,
        MAX(status_max_claimed_amount) OVER (PARTITION BY MONTH(policy_end_date)) AS policy_end_date_month_max_status_max_claimed_amo
    FROM with_windows
    WINDOW w_all_by_claim_date AS (ORDER BY claim_date DESC, id), w_all_by_reported_at AS (ORDER BY reported_at, id)
)
SELECT DISTINCT
    days_since_prev_claim_date_vs_adjuster_id_avg,
    approved_amount,
    status_avg_approved_amount
FROM with_trends
WHERE days_since_prev_claim_date_vs_adjuster_id_avg >= -40.0
ORDER BY approved_amount
