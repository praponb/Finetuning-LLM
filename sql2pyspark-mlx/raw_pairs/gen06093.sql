WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        city,
        region,
        LOWER(store_name) AS store_name_lower
    FROM stores
    WHERE opened_date >= DATE '2023-05-17'
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.store_sale_sale_date
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        store_name_lower,
        LAST_VALUE(region) IGNORE NULLS OVER (PARTITION BY MONTH(store_sale_sale_date), region ORDER BY store_sale_sale_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY store_sale_sale_date ASC, id ASC) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY YEAR(store_sale_sale_date), city ORDER BY store_sale_sale_date DESC, id ASC) AS event_seq
    FROM with_last
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.last_known_region,
        s.event_seq,
        top1.inventory_last_restock,
        top1.quantity
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.amount >= 1240
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(event_seq) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_event_seq,
        COUNT(event_seq) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_event_seq,
        SUM(event_seq) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_event_seq,
        ROW_NUMBER() OVER (ORDER BY quantity DESC, id ASC) AS seq_no
    FROM kept
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM metrics
    INTERSECT
    SELECT id
    FROM stores
    WHERE store_name LIKE 'Ol%'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.id = k.id
),
totals AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('YEAR', inventory_last_restock) AS year_start,
        AVG(quantity) AS avg_quantity,
        SUM(running_count_event_seq) AS total_running_count_event_seq,
        APPROX_PERCENTILE(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
        PERCENTILE_APPROX(quantity, 0.95) AS quantity_p95,
        SUM(running_max_event_seq) AS total_running_max_event_seq
    FROM verified
    GROUP BY CUBE (DATE_TRUNC('YEAR', inventory_last_restock))
)
SELECT
    year_start,
    avg_quantity,
    total_running_count_event_seq,
    event_seq_quartiles,
    quantity_p95,
    total_running_max_event_seq
FROM totals
ORDER BY quantity_p95 DESC NULLS LAST
