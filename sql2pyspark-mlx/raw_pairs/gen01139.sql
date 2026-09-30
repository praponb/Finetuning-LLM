WITH with_windows AS (
    SELECT
        *,
        AVG(rating) OVER w_running AS last3_avg_rating,
        COUNT(rating) OVER w_running AS last3_count_rating,
        SUM(rating) OVER w_running AS last3_sum_rating,
        MIN(rating) OVER w_running AS last3_min_rating,
        RANK() OVER (PARTITION BY city ORDER BY rating NULLS LAST) AS rating_rank
    FROM suppliers
    WHERE country = 'TH'
    WINDOW w_running AS (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    SELECT id, country
    FROM with_windows
    EXCEPT
    SELECT id, country
    FROM suppliers
    WHERE rating < 3.3
),
verified AS (
    SELECT s.*
    FROM with_windows s
    JOIN matched_keys k ON s.id = k.id AND s.country = k.country
),
valid AS (
    SELECT
        id,
        supplier_name,
        rating,
        last3_sum_rating,
        CASE WHEN rating >= 3.7 THEN 'High' WHEN rating >= 2.2 THEN 'Medium' ELSE 'Low' END AS rating_tier,
        ROUND(rating_rank / last3_sum_rating, 2) AS rating_rank_per_last3_sum_rating
    FROM verified
    WHERE supplier_name LIKE 'S%'
),
kept AS (
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND YEAR(x.launch_date) >= 2023
        )
),
metrics AS (
    SELECT
        *,
        RANK() OVER (ORDER BY rating DESC NULLS LAST) AS rating_rank,
        DENSE_RANK() OVER (ORDER BY rating_rank_per_last3_sum_rating NULLS LAST) AS rating_rank_per_last3_sum_rating_dense_rank
    FROM kept
),
eligible AS (
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.stock_qty = 289
        )
)
SELECT
    CASE WHEN rating_rank_per_last3_sum_rating_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS rating_rank_per_last3_sum_rating_dense_rank_tier,
    CASE WHEN rating_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_rank_tier,
    ROUND(AVG(rating_rank_per_last3_sum_rating_dense_rank), 2) AS avg_rating_rank_per_last3_sum_rating_dense_rank,
    ROUND(AVG(rating), 2) AS avg_rating,
    APPROX_PERCENTILE(rating_rank_per_last3_sum_rating, ARRAY(0.25, 0.5, 0.75)) AS rating_rank_per_last3_sum_rating_quartiles
FROM eligible
GROUP BY ROLLUP (CASE WHEN rating_rank_per_last3_sum_rating_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN rating_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY rating_rank_tier DESC;
