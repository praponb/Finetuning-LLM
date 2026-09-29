WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        ROUND(cost * 1.15, 2) AS adjusted_cost
    FROM shipments
    WHERE carrier IS NOT NULL
      AND order_id = 40
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY adjusted_cost DESC NULLS LAST, id) = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(adjusted_cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS next_adjusted_cost,
        LEAD(cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS next_cost,
        LAG(cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS prev_cost,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS seq_no
    FROM ranked
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.channel,
        o.amount
    FROM enriched src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT order_id, id
    FROM enriched_shipments
    EXCEPT ALL
    SELECT order_id, id
    FROM shipments
    WHERE carrier = 'FedEx'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_shipments s
    INNER JOIN matched_keys k ON s.order_id = k.order_id AND s.id = k.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT order_id, id
    FROM kept_rows
    EXCEPT DISTINCT
    SELECT order_id, id
    FROM shipments
    WHERE carrier IS NULL
),
kept_rows_shipments AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.id = k.id
),
enriched_shipments_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date,
        adjusted_cost,
        next_adjusted_cost,
        prev_cost,
        seq_no,
        channel,
        ROUND(cost / SUM(cost) OVER (PARTITION BY channel), 4) AS cost_share
    FROM kept_rows_shipments
)
-- Grouped metrics
SELECT
    channel,
    DATE_TRUNC('YEAR', shipped_date) AS year_start,
    seq_no,
    COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count,
    SUM(CASE WHEN channel = 'web' THEN prev_cost ELSE 0 END) AS web_prev_cost
FROM enriched_shipments_2
GROUP BY CUBE (channel, DATE_TRUNC('YEAR', shipped_date), seq_no)
ORDER BY channel DESC NULLS FIRST
