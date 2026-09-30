WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SIZE(tags) AS tags_count,
        COALESCE(brand, 'UNKNOWN') AS resolved_brand
    FROM products
    WHERE launch_date > DATE '2023-09-05'
      AND supplier_id IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(cost) OVER w_grp AS next_cost,
        LEAD(stock_qty) OVER w_grp AS next_stock_qty,
        LAG(stock_qty) OVER w_grp AS prev_stock_qty,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM valid
    WINDOW w_grp AS (PARTITION BY supplier_id, brand ORDER BY stock_qty, id)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        brand,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        resolved_brand,
        next_cost,
        row_seq,
        FIRST_VALUE(prev_stock_qty, TRUE) OVER (PARTITION BY YEAR(launch_date), category ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_stock_qty,
        SUM(row_seq) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq,
        MAX(row_seq) OVER (PARTITION BY brand, category) AS brand_max_row_seq
    FROM with_windows
),
joined AS (
    -- Attach suppliers attributes
    SELECT e.*, b.city
    FROM metrics e
    INNER JOIN suppliers b
        ON e.supplier_id <=> b.id
    WHERE b.country NOT IN ('JP', 'TH', 'US')
)
-- Join a dimension and aggregate
SELECT
    s.country,
    s.city AS supplier_city,
    COUNT(*) AS cnt,
    ROUND(AVG(a.stock_qty), 2) AS avg_stock_qty,
    MAX(a.row_seq) AS max_row_seq
FROM joined a
INNER JOIN suppliers s ON a.supplier_id = s.id
GROUP BY GROUPING SETS ((s.country, s.city), (s.country), ())
