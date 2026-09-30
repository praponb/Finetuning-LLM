WITH base AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_ADD(end_date, 90) AS end_date_plus_90d,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        CASE WHEN premium >= 2945.9 THEN 'HIGH' WHEN premium >= 2096.6 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier
    FROM policies
    WHERE start_date >= DATE '2024-12-02'
      AND end_date IS NOT NULL
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        premium,
        start_date,
        end_date,
        status,
        end_date_plus_90d,
        days_end_date_to_start_date,
        DATEDIFF(start_date, end_date_plus_90d) AS days_end_date_plus_90d_to_start_date
    FROM base
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        premium,
        start_date,
        end_date,
        end_date_plus_90d,
        days_end_date_plus_90d_to_start_date,
        LAST_VALUE(end_date_plus_90d) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date_plus_90d,
        LEAD(start_date) OVER w_all_by_end_date AS next_start_date,
        ROUND(AVG(days_end_date_to_start_date) OVER (PARTITION BY YEAR(end_date), DATE_TRUNC('MONTH', end_date_plus_90d)), 2) AS end_date_year_end_date_plus_90d_month_avg_days_e,
        NTILE(4) OVER (ORDER BY end_date, id) AS end_date_quartile,
        ROUND(CUME_DIST() OVER (ORDER BY end_date), 3) AS end_date_cume_dist
    FROM labeled
    WINDOW w_all_by_end_date AS (ORDER BY end_date, id)
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.policy_id = s.id) AS first_claim_date,
        (SELECT MIN(c.claimed_amount) FROM claims c WHERE c.policy_id = s.id) AS min_claim_claimed_amount
    FROM windowed s
)
-- Final projection
SELECT
    id,
    premium,
    start_date,
    end_date,
    end_date_plus_90d,
    next_start_date,
    end_date_year_end_date_plus_90d_month_avg_days_e,
    end_date_quartile,
    end_date_cume_dist,
    claim_count,
    first_claim_date,
    min_claim_claimed_amount
FROM profiled
WHERE end_date_year_end_date_plus_90d_month_avg_days_e <= 280.0
ORDER BY end_date NULLS LAST
