WITH filtered AS (
    SELECT
        id,
        supplier_name,
        city,
        rating,
        COALESCE(rating, 0) AS rating_filled,
        CASE WHEN city IN ('Tokyo', 'Chiang Mai') THEN 'Priority' ELSE 'Other' END AS city_group,
        COALESCE(city, 'NONE') AS city_clean
    FROM suppliers
    WHERE city IN ('London', 'Tokyo', 'New York')
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating_filled DESC NULLS FIRST) AS rank_in_group
        FROM filtered
    ) t
    WHERE rank_in_group = 1
),
enriched AS (
    SELECT
        id,
        supplier_name,
        city,
        rating_filled,
        city_clean,
        LAG(rating_filled) OVER w_running AS prev_rating_filled,
        LEAD(rating_filled) OVER w_running AS next_rating_filled,
        LAG(rating) OVER w_running AS prev_rating,
        LEAD(rating) OVER w_running AS next_rating,
        ROW_NUMBER() OVER w_running AS seq_no
    FROM latest
    WHERE rating_filled BETWEEN 25 AND 28
    WINDOW w_running AS (PARTITION BY city ORDER BY rating, id)
),
latest_match AS (
    SELECT
        s.id,
        s.supplier_name,
        s.city,
        s.city_clean,
        s.prev_rating_filled,
        s.next_rating_filled,
        s.next_rating,
        latest.product_category
    FROM enriched s
    JOIN LATERAL (
            SELECT c.category AS product_category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base AS (
    SELECT
        *,
        COALESCE(prev_rating_filled, 0) AS prev_rating_filled_filled,
        COALESCE(product_category, 'NONE') AS product_category_clean
    FROM latest_match
    WHERE product_category IS DISTINCT FROM 'Grocery'
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY prev_rating_filled_filled DESC) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
)
SELECT *
FROM (
    SELECT
        product_category,
        city,
        prev_rating_filled_filled
    FROM ranked
) src
PIVOT (
    SUM(prev_rating_filled_filled) AS amount,
    COUNT(1) AS cnt
    FOR city IN ('New York' AS new_york, 'Bangkok' AS bangkok)
);
