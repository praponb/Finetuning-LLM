WITH eligible AS (
    SELECT s.*
    FROM suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.price BETWEEN 380 AND 640
        )
),
eligible_suppliers AS (
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.launch_date < '2024-07-01'
        )
),
enriched AS (
    SELECT
        *,
        SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share
    FROM eligible_suppliers
),
deduped AS (
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating_share DESC, id) = 1
),
with_last AS (
    SELECT
        s.*,
        top1.product_category,
        top1.brand
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.category AS product_category, c.brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
daily AS (
    SELECT
        city,
        MIN(running_rating) AS min_running_rating,
        SUM(running_rating) AS total_running_rating
    FROM with_last
    GROUP BY CUBE (city)
)
SELECT min_running_rating
FROM daily
ORDER BY min_running_rating;
