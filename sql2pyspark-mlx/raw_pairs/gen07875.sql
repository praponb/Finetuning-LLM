WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(carrier, TRUE) OVER (PARTITION BY status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_carrier,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status, carrier), 4) AS cost_share
    FROM shipments
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.*,
        p.amount,
        p.status AS order_status
    FROM enriched e
    LEFT JOIN orders p
        ON e.order_id <=> p.id
    WHERE p.channel IN ('store', 'partner')
),
matched_keys AS (
    -- Set intersection on keys
    SELECT carrier, status
    FROM with_dims
    INTERSECT ALL
    SELECT carrier, status
    FROM shipments
    WHERE shipped_date BETWEEN '2023-03-19' AND '2024-08-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN matched_keys k ON s.carrier = k.carrier AND s.status = k.status
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY order_id ORDER BY cost_share DESC NULLS LAST) AS cost_share_rank,
        CUME_DIST() OVER w_recent AS cost_share_cume_dist,
        CUME_DIST() OVER (ORDER BY cost_share DESC NULLS LAST) AS cost_share_cume_dist_2,
        COUNT(amount) OVER w_hist AS order_id_count_amount,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share_2
    FROM verified
    WINDOW w_recent AS (PARTITION BY order_status ORDER BY cost_share DESC NULLS FIRST), w_hist AS (PARTITION BY order_id)
)
-- Join a dimension and aggregate
SELECT
    p.status AS order_status,
    s.status AS shipment_status,
    COUNT(*) AS rows_total,
    AVG(s.cost_share_cume_dist_2) AS avg_cost_share_cume_dist_2,
    MIN(s.cost) AS min_cost,
    GROUPING_ID() AS gid
FROM with_windows s
LEFT JOIN orders p ON s.order_id = p.id
GROUP BY GROUPING SETS ((p.status, s.status), (p.status));
