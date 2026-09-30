WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        amount,
        SUM(amount) OVER (PARTITION BY status, method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_amount,
        COUNT(amount) OVER (PARTITION BY status, method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount,
        AVG(amount) OVER (PARTITION BY status, method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount
    FROM payments
    WHERE paid_at < '2023-11-01'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        running_sum_amount,
        AVG(amount) OVER (PARTITION BY order_id ORDER BY running_sum_amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_amount,
        MIN(amount) OVER (PARTITION BY order_id ORDER BY running_sum_amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_amount,
        SUM(amount) OVER (PARTITION BY order_id ORDER BY running_sum_amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_amount
    FROM with_windows
),
scored_payments AS (
    -- Ranking and gap features
    SELECT
        id,
        running_sum_amount,
        last3_avg_amount,
        last3_sum_amount,
        AVG(last3_sum_amount) OVER (PARTITION BY order_id ORDER BY running_sum_amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS last3_sum_amount_avg_2_prior,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY last3_min_amount) AS last3_min_amount_cume_dist
    FROM scored
),
scored_payments_2 AS (
    -- Ranking and gap features
    SELECT
        id,
        running_sum_amount,
        last3_avg_amount,
        last3_sum_amount_avg_2_prior,
        last3_min_amount_cume_dist,
        AVG(last3_min_amount_cume_dist) OVER w_grp AS last3_min_amount_cume_dist_avg_5_prior,
        AVG(last3_avg_amount) OVER (ORDER BY last3_sum_amount_avg_2_prior, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS last3_avg_amount_avg_6_prior,
        RANK() OVER (ORDER BY last3_sum_amount_avg_2_prior) AS last3_sum_amount_avg_2_prior_rank
    FROM scored_payments
    WINDOW w_grp AS (ORDER BY last3_sum_amount_avg_2_prior, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(last3_avg_amount_avg_6_prior AS DOUBLE) AS last3_avg_amount_avg_6_prior_num
    FROM scored_payments_2
    WHERE last3_sum_amount_avg_2_prior_rank != 6
),
with_windows_payments AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(last3_min_amount_cume_dist) IGNORE NULLS OVER (ORDER BY last3_sum_amount_avg_2_prior_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last3_min_amount_cume_dist,
        SUM(running_sum_amount) OVER (ORDER BY last3_min_amount_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_sum_amount,
        RANK() OVER w_grp AS last3_avg_amount_avg_6_prior_rank
    FROM valid
    WINDOW w_grp AS (ORDER BY last3_avg_amount_avg_6_prior NULLS LAST)
),
with_windows_payments_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        running_sum_amount,
        last3_avg_amount,
        last3_min_amount_cume_dist_avg_5_prior,
        last_known_last3_min_amount_cume_dist,
        AVG(running_sum_amount) OVER w_part AS running_sum_amount_avg_6_prior,
        AVG(last3_avg_amount_avg_6_prior) OVER (ORDER BY last3_avg_amount_avg_6_prior, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last3_avg_amount_avg_6_prior_avg_6
    FROM with_windows_payments
    WINDOW w_part AS (ORDER BY last_known_last3_min_amount_cume_dist, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
)
-- Multi-level aggregation
SELECT
    CASE WHEN last3_min_amount_cume_dist_avg_5_prior >= 1 THEN 'LARGE' ELSE 'SMALL' END AS last3_min_amount_cume_dist_avg_5_prior_tier,
    ROUND(AVG(running_sum_amount), 2) AS avg_running_sum_amount,
    SUM(last3_avg_amount_avg_6_prior_avg_6) AS total_last3_avg_amount_avg_6_prior_avg_6,
    ROUND(AVG(last_known_last3_min_amount_cume_dist), 2) AS avg_last_known_last3_min_amount_cume_dist,
    COUNT(*) AS cnt
FROM with_windows_payments_2
GROUP BY CASE WHEN last3_min_amount_cume_dist_avg_5_prior >= 1 THEN 'LARGE' ELSE 'SMALL' END
HAVING ROUND(AVG(running_sum_amount), 2) >= 0
ORDER BY total_last3_avg_amount_avg_6_prior_avg_6 DESC
