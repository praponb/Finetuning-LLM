WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.unit_price) AS prior_unit_price_7d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.prior_rows_7d,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.unit_price AS move_unit_price,
        st.qty AS move_qty
    FROM joined_ranges s
    JOIN stock_moves st ON st.sku_id = s.sku_id AND st.unit_price BETWEEN 32.1 AND 372.6
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(move_moved_at) AS move_moved_at_year,
        sku_id,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month,
        MIN(moved_at) AS first_moved_at,
        MAX(prior_rows_7d) AS max_prior_rows_7d,
        MAX(move_moved_at) AS last_move_moved_at,
        COUNT(*) AS row_count,
        ROUND(STDDEV(move_unit_price), 2) AS stddev_move_unit_price
    FROM with_children_moves
    GROUP BY YEAR(move_moved_at), sku_id, DATE_TRUNC('MONTH', moved_at)
),
with_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.move_moved_at_year,
        s.sku_id,
        s.moved_at_month,
        s.first_moved_at,
        s.max_prior_rows_7d,
        s.row_count,
        s.stddev_move_unit_price,
        sk.category AS sku_category
    FROM grouped s
    LEFT JOIN skus sk ON sk.id = s.sku_id AND sk.category IS NOT NULL
)
SELECT *
FROM (
    SELECT first_moved_at, row_count, sku_category, move_moved_at_year
    FROM with_details_sku
) src
PIVOT (
    SUM(move_moved_at_year)
    FOR sku_category IN ('Toys', 'Apparel', 'Electronics')
)
