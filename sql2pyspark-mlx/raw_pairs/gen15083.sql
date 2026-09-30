WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        customer_id,
        created_at,
        attributes,
        COALESCE(ship_to.zip, 'UNKNOWN') AS zip_or_unknown,
        SIZE(FILTER(items, item -> item.qty >= 3)) AS bulk_lines,
        ship_to.city AS ship_city
    FROM carts
    WHERE customer_id IS NULL
)
-- Window metrics over several different partitions
SELECT
    id,
    created_at,
    bulk_lines,
    ship_city,
    LAG(bulk_lines) OVER w_hist AS prev_bulk_lines,
    LEAD(bulk_lines) OVER w_hist AS next_bulk_lines,
    ROW_NUMBER() OVER w_hist AS event_seq,
    RANK() OVER (PARTITION BY customer_id, ship_city ORDER BY bulk_lines NULLS LAST) AS bulk_lines_rank
FROM parsed
WINDOW w_hist AS (PARTITION BY ship_city, customer_id ORDER BY bulk_lines, id);
