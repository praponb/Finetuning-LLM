WITH prepared AS (
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
        UPPER(TRIM(claim_type)) AS claim_type_clean,
        DATE_TRUNC('WEEK', claim_date) AS claim_date_week
    FROM claims
    WHERE claimed_amount BETWEEN 17759.3 AND 29985.9
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id, status)) AS distinct_policy_id_in_adjuster_id_status,
        MAX(approved_amount) OVER (PARTITION BY adjuster_id, claim_type ORDER BY claim_date_week, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS approved_amount_max_6_prior,
        ROUND(AVG(approved_amount) OVER (PARTITION BY adjuster_id, claim_type ORDER BY claim_date_week, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_7,
        MIN(approved_amount) OVER (PARTITION BY adjuster_id, status) AS adjuster_id_status_min_approved_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_adjuster_id_status, 2) AS claimed_amount_vs_adjuster_id_status_avg,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount
    FROM prepared
    WINDOW w_adjuster_id_status AS (PARTITION BY adjuster_id, status)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(approved_amount_max_6_prior) OVER (PARTITION BY adjuster_id ORDER BY claim_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS approved_amount_max_6_prior_max_3,
        NTH_VALUE(approved_amount_max_6_prior, 2) OVER (PARTITION BY adjuster_id ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount_max_6_prior,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY status, MONTH(claim_date_week)), 2) AS approved_amount_vs_status_claim_date_week_month_,
        ROUND(approved_amount_avg_7 / NULLIF(SUM(approved_amount_avg_7) OVER (PARTITION BY status, MONTH(claim_date_week)), 0), 4) AS approved_amount_avg_7_share_of_status_claim_date,
        approved_amount_max_6_prior - LAG(approved_amount_max_6_prior) OVER (PARTITION BY adjuster_id ORDER BY claim_date DESC, id) AS approved_amount_max_6_prior_change
    FROM with_trends
),
totals AS (
    -- Summarise per group
    SELECT
        status,
        SUM(approved_amount) FILTER (WHERE status IN ('REJECTED', 'OPEN')) AS approved_amount_status_in_rejected_open,
        ROUND(100 * COUNT(CASE WHEN status IN ('OPEN', 'REJECTED', 'PAID') THEN 1 END) / COUNT(*), 1) AS pct_status_in_open_rejected_paid
    FROM with_windows
    WHERE approved_amount_avg_7_share_of_status_claim_date >= 0.1
    GROUP BY status
    HAVING AVG(second_approved_amount_max_6_prior) > 22500.0
)
SELECT status, metric_name, reading
FROM (
    SELECT status, pct_status_in_open_rejected_paid, approved_amount_status_in_rejected_open
    FROM totals
) src
UNPIVOT (
    reading FOR metric_name IN (pct_status_in_open_rejected_paid, approved_amount_status_in_rejected_open)
) unp
