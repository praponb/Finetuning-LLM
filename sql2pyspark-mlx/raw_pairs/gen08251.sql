WITH with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.sqft,
        x.order_status
    FROM stores s
    LEFT JOIN LATERAL (
            SELECT c.status AS order_status
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date <= s.opened_date
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT city, id
    FROM with_latest
    INTERSECT
    SELECT city, id
    FROM stores
    WHERE opened_date >= '2023-06-05'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest s
    INNER JOIN remaining_keys k ON s.city = k.city AND s.id = k.id
),
remaining_keys_2 AS (
    -- Set difference on keys
    SELECT id, city
    FROM kept_rows
    EXCEPT DISTINCT
    SELECT id, city
    FROM stores
    WHERE city <> 'Berlin'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN remaining_keys_2 k ON s.id = k.id AND s.city = k.city
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.quantity BETWEEN 173 AND 231
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.quantity BETWEEN 174 AND 201
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN sqft >= 16458 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    order_status,
    COUNT_IF(order_status = 'RETURNED') AS returned_events,
    SORT_ARRAY(COLLECT_SET(order_status)) AS order_status_list,
    COUNT_IF(order_status = 'PENDING') AS pending_events,
    SUM(CASE WHEN order_status = 'PENDING' THEN sqft ELSE 0 END) AS pending_sqft,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM not_flagged
GROUP BY GROUPING SETS ((CASE WHEN sqft >= 16458 THEN 'LARGE' ELSE 'SMALL' END, order_status), (CASE WHEN sqft >= 16458 THEN 'LARGE' ELSE 'SMALL' END))
ORDER BY returned_events
