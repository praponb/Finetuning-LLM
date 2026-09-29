WITH latest_match AS (
    SELECT s.*, top1.amount
    FROM stores s
    LEFT JOIN LATERAL (
            SELECT c.amount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    region,
    COUNT(*) AS row_count,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    BIT_AND(sqft) AS sqft_bit_and,
    BIT_AND(sqft) AS sqft_bit_and_2
FROM latest_match
GROUP BY region
HAVING sqft_bit_and_2 >= 1
ORDER BY sqft_bit_and_2 DESC;
