WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        status,
        RANK() OVER (PARTITION BY order_id, status ORDER BY cost DESC NULLS LAST) AS cost_rank,
        RANK() OVER (ORDER BY cost NULLS FIRST) AS cost_rank_2
    FROM shipments
),
joined AS (
    -- Attach orders attributes
    SELECT e.*, b.amount
    FROM enriched e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount NULLS LAST, id) AS rn
        FROM joined
    ) ranked
    WHERE rn = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, order_id
    FROM ranked
    INTERSECT
    SELECT id, order_id
    FROM shipments
    WHERE status = 'RETURNED'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    JOIN remaining_keys k ON s.id = k.id AND s.order_id = k.order_id
)
SELECT
    id,
    carrier,
    shipped_date,
    amount
FROM verified
ORDER BY carrier NULLS FIRST;
