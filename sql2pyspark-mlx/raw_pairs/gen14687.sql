WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(cost, 0) AS cost_filled,
        COALESCE(brand, 'UNKNOWN') AS brand_filled
    FROM products
    WHERE launch_date >= DATE '2023-04-07'
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        a.id,
        a.product_name,
        a.supplier_id,
        a.brand_filled,
        p.supplier_name
    FROM filtered a
    INNER JOIN suppliers p
        ON a.supplier_id <=> p.id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_dims s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.order_id IN (19, 29)
        )
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        s.rating,
        s.country
    FROM kept e
    LEFT JOIN suppliers s
        ON e.supplier_id = s.id
    WHERE s.supplier_name LIKE 'To%'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM joined
    EXCEPT
    SELECT id
    FROM products
    WHERE supplier_id IN (5, 9)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN remaining_keys k ON s.id = k.id
)
-- Multi-level aggregation
SELECT
    country,
    CASE WHEN rating >= 4.3 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE country = 'UK') AS uk_rows,
    COUNT_IF(country = 'DE') AS de_events,
    DENSE_RANK() OVER (PARTITION BY country ORDER BY COUNT_IF(country = 'DE') DESC) AS de_events_dense_rank
FROM kept_rows
GROUP BY country, rating_tier
