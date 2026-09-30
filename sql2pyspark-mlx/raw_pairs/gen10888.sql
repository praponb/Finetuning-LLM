WITH enriched AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.paid_at,
        e.status,
        o.discount,
        o.status AS order_status,
        o.channel
    FROM payments e
    JOIN orders o
        ON e.order_id = o.id
),
enriched_payments AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.paid_at,
        e.discount,
        e.order_status,
        b.order_date
    FROM enriched e
    JOIN orders b
        ON e.order_id <=> b.id
),
enriched_payments_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY YEAR(order_date) ORDER BY discount DESC NULLS FIRST) AS discount_cume_dist,
        AVG(discount) OVER (PARTITION BY order_id, order_status ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS discount_avg_2_prior,
        AVG(discount) OVER w_hist AS order_status_avg_discount,
        AVG(discount) OVER w_grp AS order_id_avg_discount
    FROM enriched_payments
    WINDOW w_hist AS (PARTITION BY order_status, order_id), w_grp AS (PARTITION BY order_id)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_status ORDER BY paid_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM enriched_payments_2
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        paid_at,
        discount,
        order_status,
        order_date,
        discount_avg_2_prior,
        SUM(is_new_session) OVER (PARTITION BY order_status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
enriched_payments_3 AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(discount, TRUE) OVER (PARTITION BY TO_DATE(paid_at), order_status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount,
        RANK() OVER (PARTITION BY order_status ORDER BY discount_avg_2_prior NULLS FIRST) AS discount_avg_2_prior_rank,
        NTILE(10) OVER (PARTITION BY order_id ORDER BY island_id, id) AS island_id_bucket,
        SUM(discount_avg_2_prior) OVER (PARTITION BY order_status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS discount_avg_2_prior_sum_7d
    FROM islands
)
SELECT
    id,
    order_id,
    paid_at,
    discount,
    order_status,
    last_known_discount,
    discount_avg_2_prior_rank,
    island_id_bucket,
    discount_avg_2_prior_sum_7d
FROM enriched_payments_3
ORDER BY discount_avg_2_prior_sum_7d
