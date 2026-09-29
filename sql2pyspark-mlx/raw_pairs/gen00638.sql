WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(channel) AS channel_length
    FROM orders
    WHERE order_date >= DATE '2023-09-28'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_id,
        status,
        channel,
        channel_length
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount DESC NULLS FIRST, id ASC) = 1
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.*,
        s.city,
        s.sqft
    FROM latest src
    JOIN stores s
        ON src.store_id = s.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY channel_length DESC) AS dedup_rank
        FROM joined
    ) t
    WHERE dedup_rank = 1
)
SELECT
    s.id,
    s.store_id,
    s.channel,
    s.channel_length,
    s.city,
    s.dedup_rank,
    CASE WHEN city RLIKE '^(Bangkok|Tokyo)$' THEN 'CORE' WHEN city RLIKE '^(Chiang Mai|New York)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS city_zone,
    (SELECT MAX(s2.revenue) FROM store_sales s2 WHERE s2.store_id = s.store_id) AS max_store_sale_revenue
FROM ranked s
ORDER BY channel ASC, channel_length ASC
