WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, revenue + units AS revenue_per_unit
    FROM store_sales
    WHERE sale_date >= DATE '2023-07-31'
      AND sale_date BETWEEN '2023-12-01' AND '2024-05-02'
)
-- Attach products attributes
SELECT
    s.*,
    p.product_name,
    p.launch_date,
    p.category
FROM valid s
INNER JOIN products p
    ON s.product_id = p.id;
