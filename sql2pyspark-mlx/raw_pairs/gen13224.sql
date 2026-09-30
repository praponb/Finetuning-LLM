WITH flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        customer_id,
        created_at,
        items,
        ship_to,
        attributes,
        attributes['gift'] AS gift,
        attributes['utm_source'] AS utm_source
    FROM carts
)
-- Window metrics over several different partitions
SELECT
    *,
    LAST_VALUE(utm_source, TRUE) OVER w_grp last_known_utm_source,
    LAST_VALUE(gift) IGNORE NULLS OVER w_grp last_known_gift,
    FIRST_VALUE(gift, TRUE) OVER w_grp first_known_gift
FROM flattened_attrs
WINDOW w_grp AS (PARTITION BY customer_id ORDER BY created_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
