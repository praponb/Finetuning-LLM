WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
stats AS (
    -- Group-level totals
    SELECT
        status,
        ROUND(SUM(premium), 2) AS total_premium,
        ROUND(AVG(claimed_amount), 2) AS avg_claimed_amount,
        MAX(claim_date) AS last_claim_date
    FROM joined_ranges
    GROUP BY status
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        RANK() OVER (ORDER BY avg_claimed_amount, status) AS avg_claimed_amount_rank
    FROM stats
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        status,
        total_premium,
        avg_claimed_amount,
        last_claim_date,
        rows_in_status,
        avg_claimed_amount_rank,
        MAX(avg_claimed_amount_rank) OVER w_status_avg_claimed_amount_ra AS status_avg_claimed_amount_rank_max_avg_claimed_a,
        SUM(avg_claimed_amount_rank) OVER (PARTITION BY status ORDER BY avg_claimed_amount ASC, status ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS avg_claimed_amount_rank_sum_1_prior,
        ROUND(avg_claimed_amount / NULLIF(SUM(avg_claimed_amount) OVER w_status_avg_claimed_amount_ra, 0), 4) AS avg_claimed_amount_share_of_status_avg_claimed_a,
        LAG(last_claim_date) OVER (PARTITION BY status ORDER BY avg_claimed_amount ASC, status) AS prev_last_claim_date,
        LEAD(total_premium) OVER (PARTITION BY status ORDER BY avg_claimed_amount ASC, status) AS next_total_premium
    FROM ranked
    WINDOW w_status_avg_claimed_amount_ra AS (PARTITION BY status, avg_claimed_amount_rank)
)
-- Subtotals with CUBE
SELECT
    status,
    avg_claimed_amount_rank,
    GROUPING_ID() AS gid,
    COUNT(*) AS cnt,
    COUNT(DISTINCT status) AS distinct_status,
    ROUND(AVG(total_premium), 2) AS avg_total_premium
FROM with_trends
GROUP BY CUBE(status, avg_claimed_amount_rank)
ORDER BY status NULLS LAST, avg_claimed_amount_rank NULLS LAST
