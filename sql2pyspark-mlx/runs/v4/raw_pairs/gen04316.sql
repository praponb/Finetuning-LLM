WITH exploded AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) e AS elem
),
exploded_carts AS (
    SELECT
        id,
        customer_id,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM exploded
),
joined AS (
    SELECT s.*, c.credit_limit
    FROM exploded_carts s
    JOIN customers c
        ON s.customer_id = c.id
),
base AS (
    SELECT
        *,
        CASE WHEN element = 'VIP' THEN 1 ELSE 0 END AS is_vip,
        COALESCE(element, 'DIRECT') AS element_filled
    FROM joined
    WHERE element IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        customer_id,
        element,
        credit_limit,
        is_vip,
        element_filled,
        RANK() OVER (ORDER BY credit_limit) AS credit_limit_rank,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY is_vip DESC) AS is_vip_dense_rank,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY is_vip DESC NULLS LAST) AS is_vip_dense_rank_2
    FROM base
),
with_windows_carts AS (
    SELECT
        id,
        customer_id,
        element,
        credit_limit,
        is_vip,
        element_filled,
        credit_limit_rank,
        is_vip_dense_rank,
        is_vip_dense_rank_2,
        ROUND(is_vip_dense_rank / SUM(is_vip_dense_rank) OVER (PARTITION BY customer_id), 4) AS is_vip_dense_rank_share,
        MAX(credit_limit_rank) OVER (PARTITION BY customer_id) AS customer_id_max_credit_limit_rank,
        PERCENT_RANK() OVER (ORDER BY is_vip_dense_rank DESC NULLS LAST) AS is_vip_dense_rank_percent_rank,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY customer_id), 4) AS credit_limit_share
    FROM with_windows
)
SELECT
    s.id,
    s.customer_id,
    s.element,
    s.is_vip,
    s.element_filled,
    s.credit_limit_rank,
    s.is_vip_dense_rank,
    s.is_vip_dense_rank_2,
    s.is_vip_dense_rank_percent_rank,
    (SELECT AVG(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS avg_account_balance
FROM with_windows_carts s
ORDER BY is_vip_dense_rank_2 DESC NULLS FIRST;
