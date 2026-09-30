WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        CAST(unit_price AS INT) AS unit_price_int,
        ROUND(unit_price / quantity, 2) AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price_int DESC NULLS LAST, id) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.cost,
        p.category
    FROM latest src
    LEFT JOIN products p
        ON src.product_id = p.id
),
enriched AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        a.unit_price_int,
        a.latest_rank,
        a.cost,
        a.category,
        p.stock_qty
    FROM with_dims a
    LEFT JOIN products p
        ON a.product_id <=> p.id
),
totals AS (
    -- Grouped metrics
    SELECT
        product_id,
        COUNT_IF(category = 'Sports') AS sports_events,
        COUNT_IF(category = 'Home') AS home_events,
        ROUND(AVG(cost), 2) AS avg_cost
    FROM enriched
    GROUP BY product_id
    HAVING avg_cost > 0
)
SELECT
    product_id,
    sports_events,
    home_events
FROM totals
ORDER BY product_id;
