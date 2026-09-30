WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE sku_id >= 17
      AND qty >= 168
),
joined AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price,
        sk.labels AS sku_labels
    FROM cleaned s
    JOIN skus sk ON sk.id = s.sku_id
),
derived AS (
    -- Derive helper columns
    SELECT *, FLOOR(qty / 100) * 100 AS qty_bucket
    FROM joined
),
joined_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.sku_labels,
        s.qty_bucket,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity,
        sk.sku_code
    FROM derived s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
without_matches AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM joined_2 s
    WHERE EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND w.capacity <= 11768
              AND w.city NOT IN ('Bangkok', 'Singapore')
        )
),
latest AS (
    -- Keep the latest row per warehouse_id, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        operator,
        sku_labels,
        qty_bucket,
        warehouse_opened_on,
        warehouse_city,
        warehouse_capacity,
        sku_code
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id, sku_id ORDER BY warehouse_capacity DESC, id DESC) AS recency_rank
        FROM without_matches
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    sku_id,
    sku_labels,
    qty_bucket,
    warehouse_opened_on,
    warehouse_city,
    warehouse_capacity,
    sku_code,
    LOWER(REGEXP_REPLACE(TRIM(sku_code), ' ', '_')) AS sku_code_slug
FROM latest
