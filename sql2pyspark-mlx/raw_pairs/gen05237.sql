WITH with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.city,
        s.rating,
        x.launch_date,
        x.product_price
    FROM suppliers s
    JOIN LATERAL (
            SELECT c.launch_date, c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(rating, TRUE) OVER (PARTITION BY country, city ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating,
        LEAD(rating, 1) OVER (PARTITION BY city, country ORDER BY launch_date, id) AS next_rating,
        FIRST_VALUE(rating, TRUE) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating
    FROM with_latest
    WHERE rating >= 2.7
),
enriched_suppliers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(first_known_rating) OVER w_running AS running_avg_first_known_rating,
        SUM(first_known_rating) OVER w_running AS running_sum_first_known_rating,
        MAX(first_known_rating) OVER w_running AS running_max_first_known_rating,
        AVG(next_rating) OVER (PARTITION BY city ORDER BY product_price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS next_rating_avg_6_prior
    FROM enriched
    WINDOW w_running AS (PARTITION BY country, city ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        launch_date,
        last_known_rating,
        next_rating,
        running_max_first_known_rating,
        next_rating_avg_6_prior
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY last_known_rating DESC, id) AS rn
        FROM enriched_suppliers
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    launch_date,
    last_known_rating,
    next_rating,
    running_max_first_known_rating,
    next_rating_avg_6_prior
FROM latest
ORDER BY last_known_rating DESC, next_rating_avg_6_prior DESC;
