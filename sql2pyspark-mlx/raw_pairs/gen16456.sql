WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating ASC, id ASC) AS rn
    FROM suppliers
    QUALIFY rn = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(rating) OVER (PARTITION BY country ORDER BY rating ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rating_avg_2_prior,
        AVG(rating) OVER (ORDER BY rn ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rating_avg_6,
        ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating DESC, id ASC) AS seq_no
    FROM latest
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rn ASC, id ASC) = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.product_name LIKE 'Ba%'
        )
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        country,
        seq_no
    FROM eligible
) src
PIVOT (
    SUM(seq_no) AS total,
    COUNT(1) AS n
    FOR country IN ('US', 'TH')
);
