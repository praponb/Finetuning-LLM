WITH filtered AS (
    SELECT *, ROUND(monthly_fee * 100, 2) monthly_fee_scaled
    FROM subscriptions
    WHERE start_date > DATE '2023-05-14'
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC, id) AS rank_in_group
    FROM filtered
    QUALIFY rank_in_group = 1
),
ranked_subscriptions AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, plan ORDER BY end_date DESC NULLS FIRST, id) AS row_num
        FROM ranked
    ) ranked
    WHERE row_num = 1
),
filtered_subscriptions AS (
    SELECT *, CASE WHEN status = 'EXPIRED' THEN 1 ELSE 0 END is_expired
    FROM ranked_subscriptions
    WHERE start_date >= DATE '2023-06-02'
),
with_dims AS (
    SELECT
        e.id,
        e.customer_id,
        e.plan,
        e.monthly_fee,
        e.status,
        e.rank_in_group,
        e.row_num,
        e.is_expired,
        c.segment
    FROM filtered_subscriptions e
    LEFT JOIN customers c
        ON e.customer_id <=> c.id
)
SELECT
    CASE WHEN is_expired >= 78 THEN 'LARGE' ELSE 'SMALL' END AS is_expired_tier,
    segment,
    STDDEV_SAMP(monthly_fee) AS monthly_fee_stddev,
    MAX(monthly_fee) FILTER (WHERE segment = 'Consumer') AS consumer_max_monthly_fee,
    MIN(monthly_fee) AS min_monthly_fee,
    GROUPING(CASE WHEN is_expired >= 78 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_level
FROM with_dims
GROUP BY GROUPING SETS ((CASE WHEN is_expired >= 78 THEN 'LARGE' ELSE 'SMALL' END, segment), (CASE WHEN is_expired >= 78 THEN 'LARGE' ELSE 'SMALL' END))
ORDER BY segment DESC;
