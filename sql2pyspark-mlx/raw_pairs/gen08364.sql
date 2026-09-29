WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            EXPLODE_OUTER(items) AS li_item
        FROM carts
    ) t
),
with_dims AS (
    SELECT src.*, c.segment
    FROM lines src
    JOIN customers c
        ON src.customer_id = c.id
),
cleaned AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at,
        UPPER(promo) AS promo_upper,
        COALESCE(promo, 'OTHER') AS promo_clean
    FROM with_dims
    WHERE segment IS NOT NULL
      AND promo IS NOT DISTINCT FROM 'VIP'
),
with_dims_carts AS (
    SELECT
        a.*,
        p.city,
        p.credit_limit,
        p.name
    FROM cleaned a
    JOIN customers p
        ON a.customer_id = p.id
    WHERE p.country = 'UK'
)
SELECT
    segment,
    product_id,
    SUM(CASE WHEN promo = 'WELCOME5' THEN days_since_created_at ELSE 0 END) AS welcome5_days_since_created_at,
    APPROX_PERCENTILE(qty, ARRAY(0.25, 0.5, 0.75)) AS qty_quartiles,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    ROUND(SUM(price) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS price_per_singapore,
    MIN(created_at) AS first_created_at,
    DENSE_RANK() OVER (PARTITION BY segment ORDER BY (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 DESC) AS duration_min_dense_rank
FROM with_dims_carts
GROUP BY segment, product_id
ORDER BY segment DESC NULLS LAST
