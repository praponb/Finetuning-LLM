WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        NTILE(10) OVER (PARTITION BY carrier ORDER BY cost ASC, id ASC) AS cost_bucket,
        ROUND(cost / SUM(cost) OVER (PARTITION BY DATE_TRUNC('MONTH', shipped_date), order_id), 4) AS cost_share,
        ROUND(cost / SUM(cost) OVER (PARTITION BY YEAR(shipped_date), order_id), 4) AS cost_share_2,
        CUME_DIST() OVER (PARTITION BY order_id, carrier ORDER BY cost DESC NULLS FIRST) AS cost_cume_dist
    FROM shipments
    WHERE order_id IN (11, 16, 21, 34)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_id,
        delivered_date,
        cost,
        cost_bucket,
        cost_share,
        cost_cume_dist
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY cost_share_2 DESC, id ASC) = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.delivered_date,
        src.cost,
        src.cost_bucket,
        src.cost_cume_dist,
        p.status
    FROM ranked src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM enriched
    EXCEPT
    SELECT status, id
    FROM shipments
    WHERE delivered_date >= '2024-03-26'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.status = k.status AND s.id = k.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(cost_bucket AS DOUBLE) AS cost_bucket_num
    FROM reconciled
    WHERE delivered_date >= DATE '2023-04-14'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        order_id,
        status,
        cost_cume_dist
    FROM filtered
) src
PIVOT (
    SUM(cost_cume_dist) AS amount,
    COUNT(1) AS n
    FOR status IN ('PENDING', 'COMPLETED', 'CANCELLED')
);
