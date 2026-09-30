WITH latest AS (
    SELECT *
    FROM suppliers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating ASC NULLS LAST, id ASC) = 1
),
kept AS (
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.launch_date BETWEEN '2024-10-23' AND '2025-06-01'
        )
),
scored AS (
    SELECT
        *,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_rating,
        DENSE_RANK() OVER (ORDER BY rating DESC NULLS LAST) rating_dense_rank
    FROM kept
),
latest_suppliers AS (
    SELECT
        id,
        running_rating,
        rating_dense_rank
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC NULLS FIRST, id ASC) AS dedup_rank
        FROM scored
    ) x
    WHERE dedup_rank = 1
)
SELECT
    CASE WHEN running_rating >= 20 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_rating_tier,
    CASE WHEN rating_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS rating_dense_rank_tier,
    COUNT(*) AS row_count,
    COUNT(*) AS row_count_2
FROM latest_suppliers
GROUP BY CASE WHEN running_rating >= 20 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN rating_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END
