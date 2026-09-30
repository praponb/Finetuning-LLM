WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN category = 'Grocery' THEN 1 ELSE 0 END AS is_grocery,
        COALESCE(brand, 'N/A') AS brand_filled
    FROM products
    WHERE launch_date >= DATE '2023-07-22'
      AND brand IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        product_name,
        brand,
        stock_qty,
        launch_date,
        supplier_id,
        ROUND(cost / SUM(cost) OVER (PARTITION BY supplier_id, category), 4) AS cost_share,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY price DESC, id) AS seq_no,
        COUNT(is_grocery) OVER w_grp AS brand_count_is_grocery
    FROM valid
    WHERE brand IN ('Globex', 'Umbrella')
    WINDOW w_grp AS (PARTITION BY brand)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        product_name,
        stock_qty,
        launch_date,
        cost_share,
        seq_no,
        brand_count_is_grocery
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY launch_date DESC, id) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        launch_date,
        LEAD(seq_no) OVER (ORDER BY cost_share, id) AS next_seq_no,
        LAG(seq_no) OVER (ORDER BY cost_share, id) AS prev_seq_no,
        LAG(cost_share) OVER (ORDER BY cost_share, id) AS prev_cost_share,
        LEAD(cost_share) OVER (ORDER BY cost_share, id) AS next_cost_share,
        ROW_NUMBER() OVER (ORDER BY cost_share, id) AS row_seq,
        AVG(seq_no) OVER (ORDER BY cost_share, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS seq_no_avg_2
    FROM latest
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(next_cost_share, 2) OVER w_part AS next_next_cost_share,
        LAG(prev_seq_no, 1, 0) OVER w_part AS prev_prev_seq_no,
        CUME_DIST() OVER (ORDER BY seq_no_avg_2 DESC) AS seq_no_avg_2_cume_dist
    FROM scored
    WINDOW w_part AS (ORDER BY prev_seq_no, id)
),
valid_products AS (
    -- Keep recent, valid rows
    SELECT
        id,
        launch_date,
        prev_seq_no,
        prev_cost_share,
        next_cost_share,
        row_seq,
        seq_no_avg_2,
        prev_prev_seq_no,
        seq_no_avg_2_cume_dist,
        CAST(seq_no_avg_2 AS DOUBLE) AS seq_no_avg_2_str
    FROM with_windows
    WHERE launch_date >= DATE '2023-08-08'
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.last_restock
    FROM valid_products s
    JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    prev_seq_no,
    prev_cost_share,
    next_cost_share,
    row_seq,
    seq_no_avg_2,
    prev_prev_seq_no,
    seq_no_avg_2_cume_dist,
    seq_no_avg_2_str
FROM latest_match
ORDER BY prev_seq_no NULLS FIRST
