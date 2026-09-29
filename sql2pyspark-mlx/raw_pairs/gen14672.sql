WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price * quantity AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id IN (3, 18)
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.order_date,
        d.amount,
        d.channel
    FROM cleaned e
    INNER JOIN orders d
        ON e.order_id = d.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        quantity,
        unit_price,
        line_total,
        order_date,
        amount
    FROM (
        SELECT *, RANK() OVER (PARTITION BY channel ORDER BY unit_price NULLS LAST) AS latest_rank
        FROM enriched
    ) ranked
    WHERE latest_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        line_total,
        amount,
        FIRST_VALUE(quantity, TRUE) OVER (ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
        SUM(line_total) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_line_total
    FROM ranked
    WHERE line_total < 21
),
base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(first_known_quantity / amount, 2) AS first_known_quantity_per_amount
    FROM metrics
    WHERE running_line_total <> 188
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN amount >= 1550 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
        CASE WHEN first_known_quantity_per_amount >= 84 THEN 'HIGH' ELSE 'LOW' END AS first_known_quantity_per_amount_tier,
        CASE WHEN quantity >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
        COUNT(*) AS event_count,
        ROUND(AVG(running_line_total), 2) AS avg_running_line_total,
        AVG(quantity) AS avg_quantity,
        STDDEV_SAMP(quantity) AS quantity_stddev,
        APPROX_PERCENTILE(running_line_total, ARRAY(0.25, 0.5, 0.75)) AS running_line_total_quartiles
    FROM base
    GROUP BY CASE WHEN amount >= 1550 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN first_known_quantity_per_amount >= 84 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN quantity >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING COUNT(*) > 1
)
SELECT
    amount_tier,
    first_known_quantity_per_amount_tier,
    quantity_tier,
    event_count,
    avg_running_line_total,
    quantity_stddev,
    running_line_total_quartiles
FROM stats;
