WITH lines AS (
    SELECT
        t.id,
        t.customer_id,
        t.created_at,
        t.coupon_codes,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM carts t
    LATERAL VIEW EXPLODE(t.items) li AS li_item
),
flattened AS (
    SELECT
        customer_id,
        created_at,
        qty,
        price,
        value
    FROM lines
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS value
),
joined AS (
    SELECT
        s.*,
        p.age,
        p.signup_date,
        p.credit_limit
    FROM flattened s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
metrics AS (
    SELECT
        customer_id,
        created_at,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('month', signup_date) ORDER BY qty DESC NULLS LAST) AS qty_cume_dist,
        DENSE_RANK() OVER (PARTITION BY value, customer_id ORDER BY price DESC NULLS FIRST) AS price_dense_rank,
        MAX(age) OVER (PARTITION BY YEAR(created_at), customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS age_max_1h
    FROM joined
),
joined_carts AS (
    SELECT
        e.customer_id,
        e.created_at,
        e.price_dense_rank,
        c.signup_date,
        c.name
    FROM metrics e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
cleaned AS (
    SELECT
        customer_id,
        price_dense_rank,
        signup_date,
        name,
        ADD_MONTHS(signup_date, 1) AS signup_date_plus_months
    FROM joined_carts
    WHERE YEAR(signup_date) >= 2023
),
cleaned_carts AS (
    SELECT *, CEIL(price_dense_rank) AS price_dense_rank_whole
    FROM cleaned
    WHERE customer_id IS NOT NULL
      AND signup_date_plus_months > '2024-02-17'
),
daily AS (
    SELECT
        CASE WHEN price_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS price_dense_rank_tier,
        customer_id,
        PERCENTILE_APPROX(price_dense_rank_whole, 0.95) AS price_dense_rank_whole_p95,
        APPROX_PERCENTILE(price_dense_rank, 0.5) AS price_dense_rank_p50,
        ROUND(AVG(price_dense_rank_whole), 2) AS avg_price_dense_rank_whole,
        MIN(price_dense_rank_whole) AS min_price_dense_rank_whole,
        APPROX_PERCENTILE(price_dense_rank, 0.5) AS price_dense_rank_p50_2,
        DENSE_RANK() OVER (PARTITION BY CASE WHEN price_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END ORDER BY ROUND(AVG(price_dense_rank_whole), 2) DESC) AS avg_price_dense_rank_whole_dense_rank
    FROM cleaned_carts
    GROUP BY price_dense_rank_tier, customer_id
)
SELECT
    s.price_dense_rank_tier,
    s.customer_id,
    s.price_dense_rank_whole_p95,
    s.price_dense_rank_p50,
    s.avg_price_dense_rank_whole,
    s.min_price_dense_rank_whole,
    s.price_dense_rank_p50_2,
    (SELECT MAX(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS max_account_balance
FROM daily s;
