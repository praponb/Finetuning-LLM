WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        created_at,
        ship_to,
        COALESCE(attributes['gift'], 'unknown') AS gift_or_default,
        COALESCE(attributes['utm_source'], 'unknown') AS utm_source_or_default,
        ship_to.country AS ship_country,
        COALESCE(ship_to.city, 'UNKNOWN') AS city_or_unknown,
        ARRAY_CONTAINS(MAP_KEYS(attributes), 'weight_kg') AS has_weight_kg
    FROM carts
)
-- Ranking and gap features
SELECT
    *,
    ROW_NUMBER() OVER w_ordered AS row_seq,
    ROW_NUMBER() OVER w_ordered AS seq_no,
    LAST_VALUE(gift_or_default) IGNORE NULLS OVER (PARTITION BY MONTH(created_at), ship_country ORDER BY created_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_gift_or_default,
    ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('YEAR', created_at), ship_country ORDER BY created_at DESC, id ASC) AS event_seq,
    ROW_NUMBER() OVER w_running AS row_seq_2
FROM unpacked
WINDOW w_ordered AS (PARTITION BY ship_country ORDER BY created_at ASC, id ASC), w_running AS (PARTITION BY ship_country ORDER BY created_at DESC, id ASC);
