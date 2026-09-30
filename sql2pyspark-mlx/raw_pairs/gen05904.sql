WITH enriched AS (
    SELECT
        id,
        method,
        paid_at,
        status,
        COUNT(amount) OVER (PARTITION BY MONTH(paid_at), order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_count_90d,
        CUME_DIST() OVER (PARTITION BY status, method ORDER BY amount NULLS LAST) AS amount_cume_dist,
        MAX(amount) OVER (PARTITION BY order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_max_7d,
        ROUND(amount / SUM(amount) OVER (PARTITION BY DATE_TRUNC('month', paid_at), status), 4) AS amount_share
    FROM payments
),
metrics AS (
    SELECT
        *,
        MIN(amount_max_7d) OVER (PARTITION BY status ORDER BY amount_share, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_amount_max_7d,
        COUNT(amount_max_7d) OVER (PARTITION BY status ORDER BY amount_share, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_amount_max_7d,
        MAX(amount_max_7d) OVER (PARTITION BY status ORDER BY amount_share, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_amount_max_7d
    FROM enriched
    WHERE amount_cume_dist >= 0.28
),
scored AS (
    SELECT
        *,
        LEAD(amount_max_7d) OVER (PARTITION BY status ORDER BY last7_max_amount_max_7d, id) AS next_amount_max_7d,
        LAG(amount_max_7d) OVER (PARTITION BY status ORDER BY last7_max_amount_max_7d, id) AS prev_amount_max_7d,
        LAG(amount_cume_dist) OVER (PARTITION BY status ORDER BY last7_max_amount_max_7d, id) AS prev_amount_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY last7_max_amount_max_7d, id) AS seq_no,
        ROUND(last7_count_amount_max_7d / SUM(last7_count_amount_max_7d) OVER (PARTITION BY TO_DATE(paid_at), status), 4) AS last7_count_amount_max_7d_share
    FROM metrics
),
with_windows AS (
    SELECT
        id,
        method,
        status,
        amount_count_90d,
        amount_cume_dist,
        amount_max_7d,
        amount_share,
        last7_count_amount_max_7d,
        last7_max_amount_max_7d,
        next_amount_max_7d,
        prev_amount_max_7d,
        seq_no,
        last7_count_amount_max_7d_share,
        LAG(prev_amount_cume_dist) OVER (PARTITION BY method ORDER BY amount_cume_dist, id) AS prev_prev_amount_cume_dist,
        LAG(last7_max_amount_max_7d) OVER (PARTITION BY method ORDER BY amount_cume_dist, id) AS prev_last7_max_amount_max_7d,
        LEAD(last7_max_amount_max_7d) OVER (PARTITION BY method ORDER BY amount_cume_dist, id) AS next_last7_max_amount_max_7d,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount_cume_dist, id) AS event_seq
    FROM scored
),
pivoted AS (
    SELECT *
    FROM (
        SELECT
            method,
            status,
            amount_cume_dist
        FROM with_windows
    ) src
    PIVOT (
        AVG(amount_cume_dist)
        FOR status IN ('REFUNDED' AS refunded, 'FAILED' AS failed)
    )
)
SELECT
    method,
    failed,
    CASE WHEN method RLIKE '^(bank_transfer|promptpay)$' THEN 'CORE' WHEN method RLIKE '^(card|paypal)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS method_zone
FROM pivoted;
