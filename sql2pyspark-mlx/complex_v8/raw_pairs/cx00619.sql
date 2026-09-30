WITH valid AS (
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
        reported_at
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-03-08 14:00:00'
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
      AND s.status NOT IN ('REJECTED', 'IN_REVIEW')
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count_2
    FROM qualified s
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        claim_count,
        claim_count_2,
        SUBSTRING(TRIM(claim_type), 1, 3) AS claim_type_prefix,
        CONCAT_WS('|', status, claim_type) AS status_claim_type_label,
        HOUR(reported_at) AS reported_at_hour
    FROM with_counts
    WHERE approved_amount >= 0.0
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY reported_at_hour), 3) AS reported_at_hour_cume_dist,
        ROUND(AVG(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date)), 2) AS claim_date_month_avg_approved_amount,
        MAX(claim_count) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date)) AS claim_date_month_max_claim_count,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_claim_date_month, 2) AS claimed_amount_vs_claim_date_month_avg,
        LAG(claimed_amount) OVER w_claim_type_claim_count_by_cl AS prev_claimed_amount,
        FIRST_VALUE(claimed_amount) OVER (ORDER BY reported_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount
    FROM tagged
    WINDOW w_claim_date_month AS (PARTITION BY DATE_TRUNC('MONTH', claim_date)), w_claim_type_claim_count_by_cl AS (PARTITION BY claim_type, claim_count ORDER BY claim_count_2 DESC, id)
),
claim_type_stats AS (
    -- Avg approved_amount per claim_type
    SELECT claim_type, AVG(approved_amount) AS avg_approved_amount
    FROM with_windows
    GROUP BY claim_type
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_approved_amount,
        ROUND(s.approved_amount - g.avg_approved_amount, 2) AS approved_amount_minus_avg
    FROM with_windows s
    JOIN claim_type_stats g ON g.claim_type = s.claim_type
    WHERE s.approved_amount > g.avg_approved_amount
),
tagged_2 AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_ADD(claim_date, 90) AS claim_date_plus_90d,
        SUBSTRING(TRIM(status_claim_type_label), 1, 2) AS status_claim_type_label_prefix
    FROM compared
    WHERE claim_type_prefix IS NOT NULL
)
-- Final projection
SELECT
    id,
    claim_type,
    claimed_amount,
    approved_amount,
    claim_count,
    claim_count_2,
    claim_type_prefix,
    status_claim_type_label,
    claim_date_month_avg_approved_amount,
    claim_date_month_max_claim_count,
    claimed_amount_vs_claim_date_month_avg,
    prev_claimed_amount,
    first_claimed_amount,
    avg_approved_amount,
    status_claim_type_label_prefix,
    DATEDIFF(DATE '2025-12-31', claim_date) AS days_since_claim_date
FROM tagged_2
