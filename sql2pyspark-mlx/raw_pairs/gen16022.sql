WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(cost, 0) AS cost_filled,
        SUBSTRING(carrier, 1, 4) AS carrier_prefix
    FROM shipments
    WHERE order_id IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost, id) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY latest_rank DESC, id) AS rank_in_group
        FROM ranked
    ) ranked
    WHERE rank_in_group = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        carrier,
        cost,
        carrier_prefix,
        LEAD(cost_filled, 1) OVER (PARTITION BY carrier, status ORDER BY cost, id) AS next_cost_filled,
        NTILE(3) OVER (ORDER BY cost_filled DESC, id) AS cost_filled_bucket,
        ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY cost_filled DESC, id) AS row_seq
    FROM latest
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(row_seq, 1) AS row_seq_rounded
    FROM metrics
    WHERE carrier IS NOT NULL
),
metrics_shipments AS (
    -- Running and moving aggregates
    SELECT
        id,
        carrier,
        cost,
        carrier_prefix,
        next_cost_filled,
        cost_filled_bucket,
        row_seq_rounded,
        CUME_DIST() OVER (PARTITION BY carrier ORDER BY cost DESC NULLS FIRST) AS cost_cume_dist,
        LEAD(cost, 1, 0) OVER (PARTITION BY carrier ORDER BY cost_filled_bucket, id) AS next_cost,
        RANK() OVER (PARTITION BY carrier ORDER BY next_cost_filled DESC) AS next_cost_filled_rank
    FROM filtered
    WHERE carrier = 'Kerry'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(next_cost * 1.1, 2) AS next_cost_adjusted,
        COALESCE(next_cost, 0) AS next_cost_filled_2,
        COALESCE(carrier, 'NONE') AS resolved_carrier
    FROM metrics_shipments
    WHERE carrier IS NOT NULL
      AND carrier IS DISTINCT FROM 'Kerry'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN next_cost_filled >= 90 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_cost_filled_tier,
        AVG(next_cost_filled_rank) AS avg_next_cost_filled_rank,
        ROUND(AVG(next_cost_filled_rank), 2) AS avg_next_cost_filled_rank_2,
        ROUND(AVG(next_cost_filled), 2) AS avg_next_cost_filled,
        PERCENTILE_APPROX(cost, 0.9) AS cost_p90
    FROM cleaned
    GROUP BY CASE WHEN next_cost_filled >= 90 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    next_cost_filled_tier,
    avg_next_cost_filled_rank,
    avg_next_cost_filled,
    cost_p90
FROM stats
ORDER BY cost_p90 NULLS FIRST;
