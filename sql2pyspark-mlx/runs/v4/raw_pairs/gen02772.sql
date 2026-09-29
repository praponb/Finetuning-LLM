WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        LOWER(carrier) carrier_lower,
        COALESCE(carrier, 'UNKNOWN') carrier_clean
    FROM shipments
    WHERE carrier IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY order_id ORDER BY cost NULLS LAST) cost_rank,
        NTILE(3) OVER w_hist cost_bucket,
        AVG(cost) OVER (PARTITION BY carrier, status ORDER BY delivered_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) cost_avg_6,
        LAST_VALUE(cost, TRUE) OVER (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_cost,
        SUM(cost) OVER w_grp running_cost
    FROM valid
    WINDOW w_hist AS (PARTITION BY carrier ORDER BY cost, id), w_grp AS (PARTITION BY carrier, order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY last_known_cost NULLS FIRST, id) AS rank_in_group
        FROM metrics
    ) x
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(cost) OVER (PARTITION BY status, carrier) status_min_cost,
        ROW_NUMBER() OVER (ORDER BY last_known_cost DESC, id) row_seq,
        NTILE(5) OVER (ORDER BY cost, id) cost_bucket_2
    FROM latest
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(running_cost) OVER (PARTITION BY carrier, order_id ORDER BY rank_in_group, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) running_cost_avg_6_prior,
        ROUND(running_cost / SUM(running_cost) OVER (PARTITION BY status), 4) running_cost_share
    FROM with_windows
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        p.channel,
        p.shipping_fee
    FROM scored e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier, status ORDER BY shipping_fee, id) AS row_num
        FROM enriched
    ) x
    WHERE row_num = 1
)
SELECT
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    status,
    carrier_lower,
    carrier_clean,
    cost_rank,
    CASE WHEN carrier RLIKE '^(Kerry|DHL)$' THEN 'EXTENDED' WHEN carrier RLIKE '^(FedEx|Flash)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS carrier_zone
FROM ranked
ORDER BY status DESC NULLS FIRST
