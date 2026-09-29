WITH base AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        DATEDIFF(CURRENT_DATE(), last_restock) / 7 AS weeks_since_last_restock
    FROM inventory
    WHERE last_restock >= DATE '2023-05-06'
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY weeks_since_last_restock DESC NULLS LAST) AS latest_rank
        FROM base
    ) ranked
    WHERE latest_rank = 1
),
valid AS (
    SELECT *, ROUND(weeks_since_last_restock, 1) AS weeks_since_last_restock_rounded
    FROM ranked
    WHERE last_restock >= TO_DATE('2023-07-01')
      AND quantity >= 124
),
metrics AS (
    SELECT
        *,
        SUM(weeks_since_last_restock_rounded) OVER (ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_weeks_since_last_restock_rounded,
        LAST_VALUE(quantity, TRUE) OVER (PARTITION BY product_id ORDER BY latest_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        ROW_NUMBER() OVER (ORDER BY weeks_since_last_restock ASC, id ASC) AS seq_no
    FROM valid
),
with_dims AS (
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.quantity,
        s.last_restock,
        s.latest_rank,
        s.weeks_since_last_restock_rounded,
        s.cumulative_weeks_since_last_restock_rounded,
        s.last_known_quantity,
        s.seq_no,
        b.brand,
        b.price,
        b.category
    FROM metrics s
    LEFT JOIN products b
        ON s.product_id = b.id
)
SELECT
    CASE WHEN price >= 1180 THEN 'HIGH' ELSE 'LOW' END AS price_tier,
    product_id,
    DATE_TRUNC('MONTH', last_restock) AS activity_month,
    COUNT(*) FILTER (WHERE category = 'Sports') AS sports_rows,
    SUM(quantity) FILTER (WHERE brand = 'Umbrella') AS umbrella_total_quantity,
    MAX(last_known_quantity) AS max_last_known_quantity,
    COUNT(DISTINCT id) AS distinct_ids,
    GROUPING(CASE WHEN price >= 1180 THEN 'HIGH' ELSE 'LOW' END) AS gid
FROM with_dims
GROUP BY GROUPING SETS ((CASE WHEN price >= 1180 THEN 'HIGH' ELSE 'LOW' END, product_id, DATE_TRUNC('MONTH', last_restock)), (CASE WHEN price >= 1180 THEN 'HIGH' ELSE 'LOW' END, product_id))
ORDER BY price_tier DESC NULLS LAST
