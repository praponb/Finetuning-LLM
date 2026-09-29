WITH latest AS (
    SELECT *
    FROM payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY paid_at DESC NULLS LAST, id) = 1
),
matched_keys AS (
    SELECT id, order_id
    FROM latest
    INTERSECT
    SELECT id, order_id
    FROM payments
    WHERE order_id IN (2, 24)
),
verified AS (
    SELECT s.*
    FROM latest s
    INNER JOIN matched_keys k ON s.id = k.id AND s.order_id = k.order_id
),
scored AS (
    SELECT
        order_id,
        amount,
        paid_at,
        MAX(amount) OVER w_running AS order_id_max_amount,
        DENSE_RANK() OVER (PARTITION BY method, status ORDER BY amount DESC NULLS FIRST) AS amount_dense_rank,
        PERCENT_RANK() OVER w_recent AS amount_percent_rank,
        COUNT(amount) OVER w_grp AS amount_count_7d,
        ROUND(amount / SUM(amount) OVER (PARTITION BY method), 4) AS amount_share
    FROM verified
    WHERE TO_DATE(paid_at) < '2023-04-06'
    WINDOW w_running AS (PARTITION BY order_id, status), w_recent AS (PARTITION BY status, order_id ORDER BY amount DESC), w_grp AS (PARTITION BY status, method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    SELECT *, CASE WHEN amount_percent_rank >= 0.79 THEN 'High' WHEN amount_percent_rank >= 0.23 THEN 'Medium' ELSE 'Low' END AS amount_percent_rank_level
    FROM scored
    WHERE paid_at >= DATE '2023-10-25'
      AND amount_count_7d BETWEEN 7 AND 12
),
base AS (
    SELECT
        paid_at,
        order_id_max_amount,
        amount_dense_rank,
        amount_percent_rank,
        amount_count_7d,
        amount_share,
        amount_percent_rank_level,
        amount_share + amount AS amount_share_plus_amount
    FROM cleaned
    WHERE paid_at > DATE '2023-07-26'
      AND order_id IS NOT NULL
),
valid AS (
    SELECT *, ROUND(amount_dense_rank * 1.1, 2) AS amount_dense_rank_scaled
    FROM base
    WHERE paid_at >= DATE '2023-08-13'
      AND amount_dense_rank >= 8
),
valid_payments AS (
    SELECT
        paid_at,
        order_id_max_amount,
        amount_dense_rank,
        amount_percent_rank,
        amount_count_7d,
        amount_share,
        amount_percent_rank_level,
        amount_share_plus_amount,
        amount_dense_rank_scaled,
        amount_share_plus_amount - amount_share AS amount_share_plus_amount_minus_amount_share,
        amount_dense_rank_scaled * amount_percent_rank AS amount_dense_rank_scaled_times_amount_percent_rank
    FROM valid
    WHERE paid_at > DATE '2023-06-27'
)
SELECT
    amount_dense_rank,
    amount_share,
    amount_share_plus_amount_minus_amount_share,
    amount_dense_rank_scaled_times_amount_percent_rank,
    MAX(amount_dense_rank_scaled) OVER (PARTITION BY TO_DATE(paid_at)) AS paid_at_to_date_max_amount_dense_rank_scaled,
    MAX(amount_count_7d) OVER (PARTITION BY YEAR(paid_at)) AS paid_at_year_max_amount_count_7d
FROM valid_payments;
