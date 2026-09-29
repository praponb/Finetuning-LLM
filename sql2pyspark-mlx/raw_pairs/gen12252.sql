WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status, order_id ORDER BY amount) AS row_num
        FROM payments
    ) x
    WHERE row_num = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT status, method
    FROM latest
    INTERSECT
    SELECT status, method
    FROM payments
    WHERE paid_at < '2025-10-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.status = k.status AND s.method = k.method
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.channel,
        o.discount,
        o.shipping_fee
    FROM reconciled src
    LEFT JOIN orders o
        ON src.order_id <=> o.id
    WHERE o.channel = 'mobile'
),
enriched_payments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        method,
        amount,
        status,
        row_num,
        channel,
        discount,
        shipping_fee,
        SUM(discount) OVER w_ordered discount_sum_30d,
        COUNT(shipping_fee) OVER (PARTITION BY MONTH(paid_at), channel ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) shipping_fee_count_7d,
        COUNT(discount) OVER (PARTITION BY YEAR(paid_at), method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) discount_count_90d
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    amount,
    status,
    row_num,
    shipping_fee,
    discount_sum_30d,
    discount_count_90d
FROM enriched_payments
ORDER BY discount_count_90d DESC;
