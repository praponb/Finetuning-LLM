WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.unit_price) AS prior_unit_price_1d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
deduped AS (
    -- Keep the earliest row per sku_id
    SELECT
        id,
        sku_id,
        moved_at,
        prior_rows_1d,
        prior_unit_price_1d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY prior_unit_price_1d, id) AS recency_rank
        FROM covered
    ) ranked
    WHERE recency_rank = 1
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        sku_id,
        prior_rows_1d,
        MAX(moved_at) AS last_moved_at,
        MIN(moved_at) AS first_moved_at,
        SUM(prior_rows_1d) AS total_prior_rows_1d,
        MAX(prior_unit_price_1d) AS max_prior_unit_price_1d
    FROM deduped
    WHERE prior_rows_1d BETWEEN 1 AND 8
    GROUP BY sku_id, prior_rows_1d
),
detail_rows AS (
    -- One row per move
    SELECT
        s.sku_id,
        s.prior_rows_1d,
        s.first_moved_at,
        s.total_prior_rows_1d,
        s.max_prior_unit_price_1d,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.operator AS move_operator
    FROM per_group s
    LEFT JOIN stock_moves st ON st.sku_id = s.sku_id
)
-- Final projection
SELECT
    sku_id,
    prior_rows_1d,
    first_moved_at,
    total_prior_rows_1d,
    max_prior_unit_price_1d,
    move_id,
    move_unit_price,
    move_operator
FROM detail_rows
ORDER BY move_operator, sku_id DESC NULLS FIRST
