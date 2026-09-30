WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        LEAD(unit_price) OVER w_running next_unit_price,
        LAG(quantity) OVER w_running prev_quantity,
        LEAD(quantity) OVER w_running next_quantity,
        ROW_NUMBER() OVER w_running row_seq,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY order_id, product_id), 4) unit_price_share
    FROM order_items
    WINDOW w_running AS (PARTITION BY product_id ORDER BY quantity, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(next_quantity) OVER (PARTITION BY order_id ORDER BY prev_quantity, id) next_next_quantity,
        LAG(next_quantity) OVER (PARTITION BY order_id ORDER BY prev_quantity, id) prev_next_quantity,
        LAG(quantity) OVER (PARTITION BY order_id ORDER BY prev_quantity, id) prev_quantity_2,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY prev_quantity, id) row_seq_2
    FROM metrics
    WHERE next_quantity <= 8
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        row_seq / NULLIF(unit_price_share, 0) row_seq_per_unit_price_share,
        FLOOR(row_seq_2) row_seq_2_whole
    FROM with_windows
    WHERE next_unit_price >= 360
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.*,
        b.discount,
        b.channel,
        b.amount
    FROM cleaned s
    LEFT JOIN orders b
        ON s.order_id = b.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        GREATEST(row_seq, prev_next_quantity) greatest_row_seq_prev_next_quantity,
        FLOOR(unit_price_share) unit_price_share_whole
    FROM with_dims
    WHERE channel IS NOT NULL
      AND channel IS DISTINCT FROM 'mobile'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LEAST(row_seq_2, unit_price_share) least_row_seq_2_unit_price_share,
        ROUND(row_seq_2_whole * 1.07, 2) row_seq_2_whole_scaled
    FROM filtered
    WHERE channel IS NOT NULL
),
base_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(channel, 'N/A') channel_filled
    FROM base
    WHERE next_quantity > 4
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        order_id,
        channel,
        amount
    FROM base_order_items
) src
PIVOT (
    AVG(amount) AS amount,
    COUNT(1) AS item_count
    FOR channel IN ('partner', 'store')
)
