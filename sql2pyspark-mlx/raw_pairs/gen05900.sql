WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        status
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY shipped_date DESC) AS latest_rank
        FROM shipments
    ) t
    WHERE latest_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        status,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date DESC, id) AS seq_no,
        FIRST_VALUE(carrier, TRUE) OVER (PARTITION BY status ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_carrier,
        LAST_VALUE(carrier) IGNORE NULLS OVER (ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier
    FROM latest
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY seq_no NULLS FIRST, id) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        status,
        first_known_carrier,
        last_known_carrier,
        rank_in_group,
        ROUND(rank_in_group / seq_no, 2) AS rank_in_group_per_seq_no
    FROM deduped
    WHERE order_id IN (13, 33, 40)
)
-- Join a dimension and aggregate
SELECT
    dim.channel,
    dim.status AS order_status,
    src.status AS shipment_status,
    COUNT(*) AS cnt,
    MIN(src.rank_in_group_per_seq_no) AS min_rank_in_group_per_seq_no
FROM filtered src
LEFT JOIN orders dim ON src.order_id = dim.id
GROUP BY dim.channel, dim.status, src.status
ORDER BY cnt DESC;
