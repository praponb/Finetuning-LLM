WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        LAST_DAY(start_date) AS start_date_month_end,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix
    FROM policies
    WHERE product IN ('TRAVEL', 'HEALTH', 'HOME')
      AND end_date >= DATE '2026-01-24'
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.status,
        s.status_prefix,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.adjuster_id AS claim_adjuster_id
    FROM scoped s
    JOIN claims c ON c.policy_id = s.id AND c.claimed_amount < 30001.6
),
claim_adjuster_id_stats AS (
    -- Median claim_claimed_amount per claim_adjuster_id
    SELECT claim_adjuster_id, PERCENTILE_APPROX(claim_claimed_amount, 0.5) AS median_claim_claimed_amount
    FROM detail_rows
    GROUP BY claim_adjuster_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_claim_claimed_amount,
        ROUND(s.claim_claimed_amount - g.median_claim_claimed_amount, 2) AS claim_claimed_amount_minus_median
    FROM detail_rows s
    JOIN claim_adjuster_id_stats g ON g.claim_adjuster_id = s.claim_adjuster_id
    WHERE s.claim_claimed_amount >= g.median_claim_claimed_amount
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM outliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.claim_adjuster_id
              AND c.status IN ('OPEN', 'APPROVED', 'PAID')
        )
      AND s.holder_id <= 3
)
SELECT *
FROM (
    SELECT start_date, holder_id, status, median_claim_claimed_amount
    FROM screened
) src
PIVOT (
    SUM(median_claim_claimed_amount)
    FOR status IN ('ACTIVE', 'LAPSED', 'CANCELLED', 'EXPIRED')
)
