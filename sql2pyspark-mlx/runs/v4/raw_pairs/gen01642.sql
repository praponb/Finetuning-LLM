WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM orders
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, channel ORDER BY amount DESC NULLS LAST, id) = 1
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.*,
        d.region,
        d.store_name
    FROM ranked a
    LEFT JOIN stores d
        ON a.store_id = d.id
),
joined AS (
    -- Attach stores attributes
    SELECT e.*, d.city
    FROM with_dims e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('-', status, store_name) AS status_store_name_key
    FROM joined
    WHERE region IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(amount) OVER (ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        PERCENT_RANK() OVER (ORDER BY shipping_fee DESC) AS shipping_fee_percent_rank,
        CORR(shipping_fee, discount) OVER w_part AS corr_shipping_fee_discount,
        AVG(amount) OVER (PARTITION BY channel ORDER BY discount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_avg_6_prior
    FROM filtered
    WINDOW w_part AS (PARTITION BY region)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        customer_id,
        shipping_fee_percent_rank,
        amount,
        corr_shipping_fee_discount
    FROM scored
) src
UNPIVOT (
    value FOR measure IN (shipping_fee_percent_rank, amount, corr_shipping_fee_discount AS CORR_SHIPPING_FEE_DISCOUNT)
)
