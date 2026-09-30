WITH cleaned AS (
    SELECT
        id,
        order_id,
        cost,
        status,
        LENGTH(carrier) AS carrier_length
    FROM shipments
    WHERE delivered_date >= '2023-10-01'
),
enriched AS (
    SELECT
        a.id,
        a.order_id,
        a.cost,
        a.status,
        a.carrier_length,
        p.amount
    FROM cleaned a
    LEFT JOIN orders p
        ON a.order_id <=> p.id
    WHERE p.status <> 'SHIPPED'
),
with_dims AS (
    SELECT
        s.id,
        s.order_id,
        s.amount,
        o.amount AS order_amount
    FROM enriched s
    INNER JOIN orders o
        ON s.order_id = o.id
),
metrics AS (
    SELECT
        id,
        amount,
        order_amount,
        AVG(order_amount) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS order_amount_avg_2_prior,
        CUME_DIST() OVER (ORDER BY order_amount DESC NULLS FIRST) AS order_amount_cume_dist,
        ROUND(amount / SUM(amount) OVER (PARTITION BY order_id), 4) AS amount_share,
        FIRST_VALUE(order_amount) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_order_amount,
        RANK() OVER (PARTITION BY order_id ORDER BY order_amount) AS order_amount_rank
    FROM with_dims
),
valid AS (
    SELECT *, order_amount_avg_2_prior * order_amount_cume_dist AS order_amount_avg_2_prior_times_order_amount_cume_dist
    FROM metrics
    WHERE order_amount_avg_2_prior < 896
),
valid_shipments AS (
    SELECT
        id,
        amount,
        order_amount,
        order_amount_cume_dist,
        amount_share,
        order_amount_rank,
        order_amount_avg_2_prior_times_order_amount_cume_dist,
        order_amount_cume_dist + first_known_order_amount AS order_amount_cume_dist_plus_first_known_order_amount,
        order_amount_avg_2_prior * first_known_order_amount AS order_amount_avg_2_prior_times_first_known_order_amount
    FROM valid
    WHERE amount >= 1510
),
with_windows AS (
    SELECT
        id,
        amount,
        order_amount_cume_dist,
        amount_share,
        order_amount_rank,
        order_amount_avg_2_prior_times_first_known_order_amount,
        RANK() OVER (ORDER BY amount DESC NULLS LAST) AS amount_rank,
        LAST_VALUE(order_amount_avg_2_prior_times_order_amount_cume_dist, TRUE) OVER (ORDER BY order_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_order_amount_avg_2_prior_times_order_amount_cume_dist
    FROM valid_shipments
)
SELECT
    id,
    amount,
    order_amount_rank,
    order_amount_avg_2_prior_times_first_known_order_amount,
    amount_rank
FROM with_windows
