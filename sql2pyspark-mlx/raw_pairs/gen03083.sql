WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(method, ' - ', status) AS method_status
    FROM payments
    WHERE paid_at >= DATE '2023-11-07'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(amount) OVER w_running AS last4_max_amount,
        SUM(amount) OVER w_running AS last4_sum_amount,
        COUNT(amount) OVER w_running AS last4_count_amount,
        SUM(amount) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
    FROM valid
    WINDOW w_running AS (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
with_windows_payments AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(last4_count_amount) OVER (PARTITION BY status ORDER BY last4_max_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_last4_count_amount,
        MIN(last4_count_amount) OVER (PARTITION BY status ORDER BY last4_max_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_last4_count_amount,
        AVG(last4_count_amount) OVER (PARTITION BY status ORDER BY last4_max_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_last4_count_amount,
        MAX(last4_count_amount) OVER (PARTITION BY status ORDER BY last4_max_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_last4_count_amount,
        MAX(last4_sum_amount) OVER (PARTITION BY order_id) AS order_id_max_last4_sum_amount
    FROM with_windows
    WHERE method = 'paypal'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY running_sum_last4_count_amount DESC, id) AS rank_in_group
        FROM with_windows_payments
    ) t
    WHERE rank_in_group = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        amount,
        status,
        method_status,
        last4_max_amount,
        last4_sum_amount,
        last4_count_amount,
        running_sum_last4_count_amount,
        running_avg_last4_count_amount,
        running_max_last4_count_amount,
        rank_in_group,
        running_max_last4_count_amount - last4_max_amount AS running_max_last4_count_amount_minus_last4_max_amount
    FROM deduped
    WHERE paid_at >= TO_DATE('2023-08-18')
      AND method IS NOT DISTINCT FROM 'cash'
),
valid_payments AS (
    -- Keep recent, valid rows
    SELECT
        *,
        last4_max_amount + running_max_last4_count_amount AS last4_max_amount_plus_running_max_last4_count_amount,
        ROUND(running_avg_last4_count_amount / last4_sum_amount, 2) AS running_avg_last4_count_amount_per_last4_sum_amount
    FROM base
    WHERE last4_count_amount BETWEEN 10 AND 12
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        amount,
        status,
        last4_max_amount,
        last4_sum_amount,
        running_sum_last4_count_amount,
        running_avg_last4_count_amount,
        rank_in_group,
        last4_max_amount_plus_running_max_last4_count_amount,
        SUM(last4_max_amount) OVER w_running AS running_last4_max_amount,
        SUM(running_avg_last4_count_amount_per_last4_sum_amount) OVER w_hist AS cumulative_running_avg_last4_count_amount_per_last4_sum_amount,
        LAST_VALUE(running_sum_last4_count_amount) IGNORE NULLS OVER w_ordered AS last_known_running_sum_last4_count_amount
    FROM valid_payments
    WINDOW w_running AS (PARTITION BY order_id, status ORDER BY running_sum_last4_count_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY order_id ORDER BY running_sum_last4_count_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Join a dimension and aggregate
SELECT
    o.channel,
    a.status AS payment_status,
    COUNT(*) AS num_rows,
    ROUND(SUM(a.running_last4_max_amount), 2) AS total_running_last4_max_amount
FROM scored a
INNER JOIN orders o ON a.order_id = o.id
GROUP BY o.channel, a.status
ORDER BY num_rows DESC NULLS LAST
