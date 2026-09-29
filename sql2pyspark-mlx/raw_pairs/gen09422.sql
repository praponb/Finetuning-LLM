WITH remaining_keys AS (
    -- Set difference on keys
    SELECT order_id, id
    FROM payments
    EXCEPT DISTINCT
    SELECT order_id, id
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 11 AND 16
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM payments s
    JOIN remaining_keys k ON s.order_id = k.order_id AND s.id = k.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT src.*, d.shipping_fee
    FROM reconciled src
    JOIN orders d
        ON src.order_id <=> d.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        RANK() OVER (PARTITION BY order_id, status ORDER BY shipping_fee DESC NULLS FIRST) shipping_fee_rank,
        SUM(amount) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) amount_sum_30d,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY amount DESC) amount_percent_rank,
        STDDEV_SAMP(shipping_fee) OVER (PARTITION BY method, order_id) method_stddev_samp_shipping_fee
    FROM with_dims
    WHERE method IN ('card', 'bank_transfer')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, shipping_fee_rank / NULLIF(amount_percent_rank, 0) shipping_fee_rank_per_amount_percent_rank
    FROM enriched
    WHERE order_id IS NOT NULL
),
cleaned_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, shipping_fee_rank_per_amount_percent_rank - shipping_fee_rank shipping_fee_rank_per_amount_percent_rank_minus_shipping_fee_rank
    FROM cleaned
    WHERE order_id IS NOT NULL
)
-- Multi-level aggregation
SELECT
    order_id,
    APPROX_PERCENTILE(amount_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS amount_percent_rank_quartiles,
    MIN(shipping_fee_rank) AS min_shipping_fee_rank,
    PERCENTILE_APPROX(shipping_fee_rank, 0.5) AS shipping_fee_rank_p50,
    MIN(amount_sum_30d) AS min_amount_sum_30d
FROM cleaned_payments
GROUP BY order_id
HAVING MIN(shipping_fee_rank) > 0;
