WITH valid AS (
    SELECT *, SUBSTRING(city, 1, 4) AS city_prefix
    FROM suppliers
    WHERE city IS NOT NULL
      AND rating > 3.7
),
scored AS (
    SELECT
        *,
        AVG(rating) OVER (PARTITION BY country) AS country_avg_rating,
        AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rating_avg_2,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC NULLS FIRST) AS rating_dense_rank,
        ROW_NUMBER() OVER (ORDER BY rating DESC, id) AS seq_no
    FROM valid
),
with_latest AS (
    SELECT s.*, top1.cost
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    SELECT
        *,
        SUM(rating_avg_2) OVER (PARTITION BY country ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating_avg_2,
        NTILE(5) OVER (PARTITION BY country ORDER BY cost DESC, id) AS cost_bucket,
        NTILE(3) OVER (PARTITION BY country ORDER BY rating_avg_2, id) AS rating_avg_2_bucket,
        LAG(cost, 1) OVER (PARTITION BY country ORDER BY rating_dense_rank, id) AS prev_cost
    FROM with_latest
    WHERE city != 'Chiang Mai'
)
SELECT
    country,
    seq_no,
    CASE WHEN rating_avg_2 >= 4 THEN 'HIGH' ELSE 'LOW' END AS rating_avg_2_tier,
    ROUND(SUM(rating_avg_2) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS rating_avg_2_per_singapore,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count,
    COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count
FROM metrics
GROUP BY country, seq_no, CASE WHEN rating_avg_2 >= 4 THEN 'HIGH' ELSE 'LOW' END
ORDER BY jp_count;
