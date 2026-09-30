WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(amount, 2) AS amount_rounded
    FROM orders
    WHERE order_date >= DATE '2023-10-02'
      AND store_id IS NOT NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.signup_date,
        b.city,
        b.credit_limit
    FROM valid s
    JOIN customers b
        ON s.customer_id = b.id
    WHERE b.age >= 60
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_dims s
    WHERE EXISTS (
            SELECT 1
            FROM payments x
            WHERE x.order_id = s.id
              AND x.method <> 'bank_transfer'
        )
),
with_dims_orders AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.name,
        c.email
    FROM eligible a
    JOIN customers c
        ON a.customer_id = c.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(credit_limit) OVER (PARTITION BY store_id, customer_id ORDER BY amount_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_credit_limit,
        NTILE(3) OVER (ORDER BY amount_rounded DESC, id) AS amount_rounded_bucket,
        PERCENT_RANK() OVER (ORDER BY shipping_fee NULLS LAST) AS shipping_fee_percent_rank
    FROM with_dims_orders
)
SELECT
    id,
    customer_id,
    store_id,
    order_date,
    status,
    amount,
    discount,
    shipping_fee,
    amount_rounded,
    CASE WHEN status RLIKE '^(COMPLETED|PENDING)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS status_zone
FROM enriched
ORDER BY shipping_fee NULLS LAST, discount DESC;
