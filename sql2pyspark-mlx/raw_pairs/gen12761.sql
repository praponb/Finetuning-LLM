WITH filtered AS (
    SELECT
        *,
        amount - discount AS amount_minus_discount,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM orders
    WHERE customer_id IS NOT NULL
)
SELECT *
FROM (
    SELECT
        customer_id,
        channel,
        amount
    FROM filtered
) src
PIVOT (
    AVG(amount) AS value_sum,
    COUNT(1) AS n
    FOR channel IN ('mobile' AS mobile, 'store' AS store, 'web' AS web)
);
