WITH enriched AS (
    SELECT
        *,
        FIRST_VALUE(method, TRUE) OVER (PARTITION BY YEAR(paid_at) ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method,
        LAST_VALUE(amount, TRUE) OVER (PARTITION BY method, status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        SUM(amount) OVER (PARTITION BY order_id, method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
    FROM payments
),
flagged AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM enriched
),
islands AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY island_id DESC) AS rank_in_group
        FROM islands
    ) t
    WHERE rank_in_group = 1
),
flagged_2 AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM latest
),
sessions_payments AS (
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        island_id,
        rank_in_group,
        SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged_2
),
latest_payments AS (
    SELECT *
    FROM sessions_payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC NULLS LAST, id) = 1
)
SELECT
    d.channel,
    COUNT(*) AS cnt,
    MIN(src.amount) AS min_amount,
    ROUND(SUM(src.session_no), 2) AS total_session_no,
    GROUPING_ID() AS grouping_level
FROM latest_payments src
LEFT JOIN orders d ON src.order_id = d.id
GROUP BY CUBE (d.channel)
ORDER BY total_session_no DESC;
