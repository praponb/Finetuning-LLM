WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        DATE_FORMAT(reported_at, 'MMM yyyy') AS reported_at_label,
        MONTH(reported_at) AS reported_at_month,
        DATE_TRUNC('WEEK', reported_at) AS reported_at_week
    FROM claims
    WHERE claimed_amount BETWEEN 5313.8 AND 42838.9
      AND claim_type IS NOT NULL
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claim_type NOT IN ('delay', 'medical')
              AND c.claim_date BETWEEN DATE '2024-03-24' AND DATE '2025-11-08'
        )
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY claim_type ORDER BY reported_at_week DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(reported_at_month / NULLIF(SUM(reported_at_month) OVER (PARTITION BY claim_type), 0), 4) AS reported_at_month_share_of_claim_type,
        ROW_NUMBER() OVER (PARTITION BY MONTH(reported_at_week) ORDER BY reported_at, id) AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type ORDER BY reported_at_week DESC), 3) AS reported_at_week_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY reported_at_week DESC, id) AS seq_no,
        NTILE(4) OVER (PARTITION BY MONTH(reported_at_week) ORDER BY reported_at, id) AS reported_at_quartile
    FROM screened
),
status_stats AS (
    -- Median approved_amount per status
    SELECT status, PERCENTILE_APPROX(approved_amount, 0.5) AS median_approved_amount
    FROM with_windows
    GROUP BY status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_approved_amount,
        ROUND(s.approved_amount - g.median_approved_amount, 2) AS approved_amount_minus_median
    FROM with_windows s
    JOIN status_stats g ON g.status = s.status
    WHERE s.approved_amount >= g.median_approved_amount
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, reported_at_quartile, running_count, reported_at_month, seq_no
    FROM above_average
) src
UNPIVOT (
    amount FOR field IN (reported_at_quartile AS m_reported_at_quartile, running_count AS m_running_count, reported_at_month AS reported_at_month_v, seq_no AS seq_no_v)
) unp
ORDER BY id, field
