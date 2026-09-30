WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        CASE WHEN unit_price >= 360 THEN 'High' WHEN unit_price >= 160 THEN 'Medium' ELSE 'Low' END AS unit_price_tier
    FROM order_items
    WHERE product_id IS NULL
),
cleaned_order_items AS (
    -- Keep recent, valid rows
    SELECT *, unit_price - quantity AS line_total
    FROM cleaned
    WHERE unit_price >= 190
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        unit_price_tier,
        line_total,
        CASE WHEN unit_price >= 410 THEN 'High' WHEN unit_price >= 160 THEN 'Medium' ELSE 'Low' END AS unit_price_band,
        quantity - line_total AS quantity_minus_line_total
    FROM cleaned_order_items
    WHERE quantity BETWEEN 5 AND 7
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, quantity * line_total AS quantity_times_line_total
    FROM base
    WHERE order_id IN (1, 7, 17)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity, id) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, unit_price - dedup_rank AS unit_price_minus_dedup_rank
    FROM ranked
    WHERE line_total <= 51
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity, id) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
)
-- Join a dimension and aggregate
SELECT
    dim.channel,
    dim.status,
    COUNT(*) AS cnt,
    ROUND(SUM(e.quantity_minus_line_total), 2) AS total_quantity_minus_line_total
FROM deduped e
INNER JOIN orders dim ON e.order_id = dim.id
GROUP BY dim.channel, dim.status
ORDER BY status NULLS LAST;
