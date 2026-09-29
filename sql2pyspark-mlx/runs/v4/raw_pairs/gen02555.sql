WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY start_date NULLS LAST) AS rank_in_group
    FROM subscriptions
    QUALIFY rank_in_group = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY rank_in_group DESC, id) AS rn
    FROM latest
    QUALIFY rn = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        rank_in_group,
        rn,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket,
        CUME_DIST() OVER w_hist AS monthly_fee_cume_dist
    FROM deduped
    WINDOW w_hist AS (ORDER BY monthly_fee NULLS LAST)
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.credit_limit,
        c.email,
        c.segment
    FROM scored src
    LEFT JOIN customers c
        ON src.customer_id = c.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN monthly_fee_cume_dist >= 0.75 THEN 'PREMIUM' ELSE 'STANDARD' END AS monthly_fee_cume_dist_tier,
    CASE WHEN monthly_fee >= 233 THEN 'PREMIUM' ELSE 'STANDARD' END AS monthly_fee_tier,
    AVG(monthly_fee) FILTER (WHERE status = 'CANCELLED') AS cancelled_avg_monthly_fee,
    SUM(CASE WHEN segment = 'Consumer' THEN monthly_fee ELSE 0 END) AS consumer_monthly_fee,
    SUM(credit_limit) FILTER (WHERE segment = 'SMB') AS smb_total_credit_limit,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) AS active_count,
    COUNT(*) AS num_rows,
    GROUPING(CASE WHEN monthly_fee_cume_dist >= 0.75 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_id
FROM with_dims
GROUP BY GROUPING SETS ((CASE WHEN monthly_fee_cume_dist >= 0.75 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN monthly_fee >= 233 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN monthly_fee_cume_dist >= 0.75 THEN 'PREMIUM' ELSE 'STANDARD' END))
HAVING consumer_monthly_fee < 0
