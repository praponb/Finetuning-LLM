WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
filtered AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM in_window s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.product IS NOT NULL
        )
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        status,
        approved_amount,
        product,
        NTILE(5) OVER (PARTITION BY policy_id ORDER BY claimed_amount, id) AS claimed_amount_quintile,
        approved_amount - LAG(approved_amount) OVER (PARTITION BY MONTH(claim_date) ORDER BY premium, id) AS approved_amount_change
    FROM filtered
),
status_stats AS (
    -- Avg claimed_amount per status
    SELECT status, AVG(claimed_amount) AS avg_claimed_amount
    FROM with_trends
    GROUP BY status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claimed_amount,
        ROUND(s.claimed_amount - g.avg_claimed_amount, 2) AS claimed_amount_minus_avg
    FROM with_trends s
    JOIN status_stats g ON g.status = s.status
    WHERE s.claimed_amount > g.avg_claimed_amount
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, avg_claimed_amount, claimed_amount_minus_avg, claimed_amount, approved_amount
    FROM outliers
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric_name IN (avg_claimed_amount, claimed_amount_minus_avg, claimed_amount, approved_amount)
) unp
