WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        QUARTER(paid_at) AS paid_at_quarter,
        COALESCE(method, 'Unknown') AS method_filled,
        COALESCE(method, 'OTHER') AS resolved_method
    FROM payments
    WHERE method IS NOT NULL
      AND order_id IN (8, 20, 26)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_at_label,
        CASE WHEN method IN ('card', 'paypal') THEN 'Priority' ELSE 'Other' END AS method_group,
        COALESCE(method, 'DIRECT') AS method_filled_2
    FROM cleaned
    WHERE method IS DISTINCT FROM 'paypal'
      AND order_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY paid_at_quarter NULLS LAST) AS paid_at_quarter_rank,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY paid_at_quarter DESC NULLS LAST) AS paid_at_quarter_percent_rank
    FROM valid
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        method,
        amount,
        status,
        paid_at_quarter,
        paid_at_label,
        method_filled_2,
        paid_at_quarter_rank,
        paid_at_quarter_percent_rank
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY paid_at_quarter_percent_rank NULLS FIRST, id) = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    status,
    metric,
    reading
FROM (
    SELECT
        id,
        status,
        paid_at_quarter_percent_rank,
        amount
    FROM deduped
) src
UNPIVOT (
    reading FOR metric IN (paid_at_quarter_percent_rank, amount)
)
