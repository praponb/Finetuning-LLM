WITH prepared AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        CONCAT_WS('-', status, claim_type) AS status_claim_type_label,
        DATEDIFF(DATE '2026-01-01', reported_at) AS days_since_reported_at
    FROM claims
    WHERE status = 'PAID'
      AND YEAR(claim_date) = 2024
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_claim_type_by_claim_date) AS days_since_prev_claim_date,
        LEAD(claim_date, 3) OVER w_claim_type_by_claim_date AS next3_claim_date,
        SUM(days_since_reported_at) OVER (PARTITION BY status, adjuster_id) AS status_adjuster_id_sum_days_since_reported_at,
        COUNT(*) OVER (PARTITION BY status, adjuster_id) AS rows_in_status_adjuster_id,
        LAST_VALUE(approved_amount) OVER (PARTITION BY policy_id, adjuster_id ORDER BY days_since_reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_approved_amount,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type ORDER BY claim_date NULLS LAST), 3) AS claim_date_percent_rank,
        SUM(approved_amount) OVER (PARTITION BY policy_id, adjuster_id ORDER BY days_since_reported_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS approved_amount_sum_3
    FROM prepared
    WINDOW w_claim_type_by_claim_date AS (PARTITION BY claim_type ORDER BY claim_date NULLS LAST, id)
),
summary AS (
    -- Group-level totals
    SELECT
        claim_type,
        COUNT(*) FILTER (WHERE status IN ('IN_REVIEW', 'OPEN')) AS n_status_in_in_review_open,
        SUM(CASE WHEN status IN ('REJECTED', 'APPROVED', 'OPEN') THEN 1 ELSE 0 END) AS n_status_in_rejected_approved,
        COUNT(*) FILTER (WHERE claim_type NOT IN ('delay', 'fire')) AS n_claim_type_not_in_delay_fire,
        COUNT(*) AS cnt,
        ROUND(SUM(approved_amount) / NULLIF(SUM(rows_in_status_adjuster_id), 0), 3) AS approved_amount_to_rows_in_status_adjuster_id_ra
    FROM with_windows
    WHERE status_adjuster_id_sum_days_since_reported_at < 5600
    GROUP BY claim_type
)
SELECT
    claim_type,
    n_status_in_in_review_open,
    n_status_in_rejected_approved,
    n_claim_type_not_in_delay_fire,
    cnt,
    approved_amount_to_rows_in_status_adjuster_id_ra,
    LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
FROM summary
WHERE n_claim_type_not_in_delay_fire BETWEEN 1 AND 10
ORDER BY approved_amount_to_rows_in_status_adjuster_id_ra DESC NULLS FIRST, cnt DESC
