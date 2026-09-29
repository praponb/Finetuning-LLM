WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_30d,
        SUM(b.approved_amount) AS prior_approved_amount_30d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 30)
    GROUP BY a.id, a.policy_id, a.claim_date
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM joined_ranges s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
        )
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date,
        LAST_DAY(claim_date) AS claim_date_month_end,
        ABS(prior_rows_30d - 3) AS prior_rows_30d_distance
    FROM without_matches
    WHERE claim_date IS NOT NULL
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_30d,
    prior_approved_amount_30d,
    days_since_claim_date,
    claim_date_month_end,
    prior_rows_30d_distance,
    DATE_TRUNC('MONTH', claim_date_month_end) AS claim_date_month_end_month,
    ABS(prior_rows_30d - 2) AS prior_rows_30d_distance_2
FROM scored
ORDER BY claim_date NULLS LAST
