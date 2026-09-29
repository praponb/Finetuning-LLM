WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN status IN ('REFUNDED', 'FAILED') THEN 'Priority' ELSE 'Other' END AS status_group,
        CASE WHEN status IN ('SUCCESS', 'FAILED') THEN 'Priority' ELSE 'Other' END AS status_group_2
    FROM payments
    WHERE method IS DISTINCT FROM 'bank_transfer'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method, status ORDER BY paid_at NULLS FIRST, id) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        SUM(amount) OVER (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        NTILE(10) OVER (PARTITION BY method ORDER BY amount DESC, id) AS amount_bucket,
        SUM(amount) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount_2,
        AVG(amount) OVER w_grp AS amount_avg_3
    FROM latest
    WHERE TO_DATE(paid_at) < '2024-06-01'
    WINDOW w_grp AS (ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        cumulative_amount,
        amount_bucket,
        amount_avg_3,
        COUNT(amount_bucket) OVER (PARTITION BY order_id ORDER BY amount_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount_bucket,
        MAX(amount_bucket) OVER (PARTITION BY order_id ORDER BY amount_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount_bucket,
        SUM(amount_bucket) OVER (PARTITION BY order_id ORDER BY amount_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_amount_bucket,
        MIN(amount_bucket) OVER (PARTITION BY order_id ORDER BY amount_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount_bucket,
        FIRST_VALUE(cumulative_amount) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY cumulative_amount_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_amount
    FROM scored
)
-- Grouped metrics
SELECT
    order_id,
    CASE WHEN running_max_amount_bucket >= 9.0 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_max_amount_bucket_tier,
    ROUND(AVG(amount_bucket), 2) AS avg_amount_bucket,
    COUNT(*) AS num_rows,
    ROUND(AVG(amount_avg_3), 2) AS avg_amount_avg_3,
    APPROX_PERCENTILE(amount_bucket, 0.5) AS amount_bucket_p50,
    PERCENTILE_APPROX(amount_bucket, 0.9) AS amount_bucket_p90,
    PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY PERCENTILE_APPROX(amount_bucket, 0.9) DESC) AS amount_bucket_p90_percent_rank
FROM enriched
GROUP BY order_id, running_max_amount_bucket_tier
HAVING COUNT(*) > 1
ORDER BY running_max_amount_bucket_tier DESC;
