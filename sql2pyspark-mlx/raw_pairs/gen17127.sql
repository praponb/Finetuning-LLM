WITH flattened_attrs AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        items,
        attributes,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
        attributes['gift'] AS gift
    FROM carts
    WHERE customer_id IN (7, 13, 27)
)
SELECT
    customer_id,
    COUNT(DISTINCT gift) AS distinct_gifts,
    COUNT(*) AS num_rows
FROM flattened_attrs
GROUP BY customer_id;
