WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        city,
        segment,
        signup_date,
        credit_limit,
        CONCAT_WS(',', name, country) AS name_country_key
    FROM customers
    WHERE signup_date > TO_DATE('2023-05-29')
      AND city IS NOT NULL
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.order_shipping_fee,
        latest.order_discount
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.shipping_fee AS order_shipping_fee, c.discount AS order_discount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(credit_limit) OVER (PARTITION BY city ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_credit_limit,
        SUM(credit_limit) OVER (PARTITION BY city ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_credit_limit,
        COUNT(credit_limit) OVER (PARTITION BY city ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_credit_limit,
        SUM(order_discount) OVER (PARTITION BY city ORDER BY order_discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_order_discount
    FROM with_latest
    WHERE segment IN ('Consumer', 'Corporate', 'SMB')
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.started_at >= s.signup_date
        )
),
with_latest_customers AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.session_browser,
        latest.session_started_at
    FROM eligible s
    JOIN LATERAL (
            SELECT c.browser AS session_browser, c.started_at AS session_started_at
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN session_started_at - LAG(session_started_at) OVER (PARTITION BY segment ORDER BY session_started_at, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_latest_customers
),
sessions_customers AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        city,
        segment,
        signup_date,
        running_sum_credit_limit,
        running_order_discount,
        session_started_at,
        SUM(is_new_burst) OVER (PARTITION BY segment ORDER BY session_started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Grouped metrics
SELECT
    CASE WHEN running_sum_credit_limit >= 83000 THEN 'HIGH' ELSE 'LOW' END AS running_sum_credit_limit_tier,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    MIN(session_started_at) AS first_session_started_at,
    MAX(session_started_at) AS ended_at
FROM sessions_customers
GROUP BY CUBE (CASE WHEN running_sum_credit_limit >= 83000 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY running_sum_credit_limit_tier DESC;
