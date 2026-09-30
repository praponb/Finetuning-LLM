WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY quantity DESC NULLS LAST, id) AS dedup_rank
        FROM order_items
    ) ranked
    WHERE dedup_rank = 1
),
with_dims AS (
    SELECT s.*, p.brand
    FROM deduped s
    LEFT JOIN products p
        ON s.product_id = p.id
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, brand ORDER BY dedup_rank DESC, id) AS latest_rank
        FROM with_dims
    ) x
    WHERE latest_rank = 1
),
valid AS (
    SELECT
        *,
        ROUND(dedup_rank, 2) AS dedup_rank_rounded,
        COALESCE(unit_price, 0) AS unit_price_filled
    FROM ranked
    WHERE brand IS NOT NULL
),
base AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        dedup_rank,
        brand,
        LEAST(dedup_rank_rounded, unit_price) AS least_dedup_rank_rounded_unit_price
    FROM valid
    WHERE quantity BETWEEN 6 AND 7
),
with_dims_order_items AS (
    SELECT
        src.*,
        b.price,
        b.category
    FROM base src
    INNER JOIN products b
        ON src.product_id <=> b.id
    WHERE b.category = 'Grocery'
),
joined AS (
    SELECT
        src.id,
        src.order_id,
        src.product_id,
        src.quantity,
        src.unit_price,
        src.dedup_rank,
        src.price,
        d.discount,
        d.amount,
        d.status
    FROM with_dims_order_items src
    LEFT JOIN orders d
        ON src.order_id = d.id
)
SELECT
    CASE WHEN price >= 960 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
    CASE WHEN unit_price >= 360 THEN 'PREMIUM' ELSE 'STANDARD' END AS unit_price_tier,
    product_id,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    SUM(CASE WHEN status = 'PENDING' THEN discount ELSE 0 END) AS pending_discount,
    COUNT(CASE WHEN status = 'SHIPPED' THEN 1 END) AS shipped_count,
    APPROX_PERCENTILE(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles
FROM joined
GROUP BY GROUPING SETS ((CASE WHEN price >= 960 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN unit_price >= 360 THEN 'PREMIUM' ELSE 'STANDARD' END, product_id), (CASE WHEN price >= 960 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN unit_price >= 360 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN price >= 960 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY avg_quantity DESC;
