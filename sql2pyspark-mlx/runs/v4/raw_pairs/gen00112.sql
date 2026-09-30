WITH flattened AS (
    SELECT
        id,
        created_at,
        line.qty,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) li AS line
)
SELECT
    id,
    created_at,
    qty,
    price,
    promo,
    SUM(price) OVER (PARTITION BY promo) AS promo_sum_price,
    PERCENT_RANK() OVER (PARTITION BY promo ORDER BY qty ASC) AS qty_percent_rank,
    PERCENT_RANK() OVER (PARTITION BY promo ORDER BY price DESC) AS price_percent_rank,
    ROUND(price / SUM(price) OVER (PARTITION BY promo), 4) AS price_share
FROM flattened;
