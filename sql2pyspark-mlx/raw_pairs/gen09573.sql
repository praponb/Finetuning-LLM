WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        UPPER(product_name) AS product_name_upper
    FROM products
    WHERE supplier_id IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost DESC NULLS FIRST, id ASC) AS rn
    FROM valid
    QUALIFY rn = 1
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        b.rating,
        b.country
    FROM deduped src
    LEFT JOIN suppliers b
        ON src.supplier_id = b.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY rn DESC, id ASC) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(cost) OVER (PARTITION BY brand ORDER BY price ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cost_avg_6,
        NTILE(3) OVER (PARTITION BY country ORDER BY price DESC, id ASC) AS price_bucket,
        LEAD(rating, 1) OVER (PARTITION BY brand, country ORDER BY price ASC, id ASC) AS next_rating,
        ROUND(price / SUM(price) OVER (PARTITION BY brand, supplier_id), 4) AS price_share,
        AVG(price) OVER (ORDER BY price ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS price_avg_2_prior
    FROM ranked
    WHERE rn = 1
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.quantity
    FROM scored s
    JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id, brand ORDER BY price_avg_2_prior DESC, id ASC) = 1
)
SELECT
    id,
    category,
    brand,
    price,
    cost,
    launch_date,
    tags,
    supplier_id,
    rating,
    CASE WHEN brand RLIKE '^(Stark|Umbrella)$' THEN 'GROUP_B' WHEN brand RLIKE '^(Acme|Globex)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS brand_zone
FROM latest
ORDER BY category DESC
