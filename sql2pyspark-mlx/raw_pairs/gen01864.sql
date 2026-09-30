WITH cleaned AS (
    SELECT
        id,
        order_id,
        amount,
        DATEDIFF(CURRENT_DATE(), paid_at) / 365.25 AS years_since_paid_at
    FROM payments
    WHERE paid_at >= TO_DATE('2023-06-04')
      AND method NOT IN ('cash', 'promptpay')
),
joined AS (
    SELECT
        a.id,
        a.order_id,
        a.years_since_paid_at,
        p.discount,
        p.status,
        p.channel
    FROM cleaned a
    LEFT JOIN orders p
        ON a.order_id = p.id
    WHERE p.status != 'PENDING'
),
metrics AS (
    SELECT
        id,
        discount,
        status,
        channel,
        AVG(discount) OVER (PARTITION BY order_id ORDER BY discount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS discount_avg_6_prior,
        LEAD(discount, 1) OVER (PARTITION BY order_id ORDER BY years_since_paid_at, id) AS next_discount,
        RANK() OVER (PARTITION BY order_id ORDER BY discount NULLS LAST) AS discount_rank,
        SUM(years_since_paid_at) OVER (PARTITION BY channel ORDER BY years_since_paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_years_since_paid_at,
        NTILE(10) OVER (PARTITION BY order_id ORDER BY discount DESC, id) AS discount_bucket
    FROM joined
),
base AS (
    SELECT *, CONCAT_WS('/', channel, status) AS channel_status_key
    FROM metrics
    WHERE status IS NOT NULL
      AND discount_avg_6_prior = 0
)
SELECT *
FROM (
    SELECT
        id,
        channel,
        discount,
        next_discount
    FROM base
) src
UNPIVOT (
    val FOR metric IN (discount, next_discount)
);
