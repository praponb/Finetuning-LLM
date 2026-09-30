WITH parsed AS (
    SELECT
        id,
        customer_id,
        EXISTS(items, li -> li.price > 100) AS has_premium_item,
        attributes['utm_source'] AS utm_source,
        COALESCE(attributes['gift'], 'none') AS gift_or_default,
        COALESCE(attributes['utm_source'], 'direct') AS utm_source_or_default,
        COALESCE(ship_to.city, 'UNKNOWN') AS city_or_unknown
    FROM carts
)
SELECT
    customer_id,
    COUNT(DISTINCT gift_or_default) AS distinct_gift_or_defaults,
    COUNT(*) AS event_count,
    COUNT(*) AS cnt,
    COUNT(DISTINCT gift_or_default) AS distinct_gift_or_defaults_2,
    COUNT(DISTINCT utm_source) AS distinct_utm_sources,
    GROUPING(customer_id) AS grouping_level
FROM parsed
GROUP BY CUBE (customer_id)
HAVING COUNT(DISTINCT gift_or_default) >= 1
ORDER BY cnt DESC
