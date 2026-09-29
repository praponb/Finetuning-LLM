WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        SUBSTRING(status, 1, 3) AS status_prefix
    FROM shipments
    WHERE shipped_date > DATE '2023-07-16'
      AND order_id IN (2, 17, 20, 31)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC NULLS FIRST, id ASC) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT *, DAY(shipped_date) AS shipped_date_day
    FROM ranked
    WHERE delivered_date >= DATE '2023-08-02'
      AND carrier IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status_prefix,
        latest_rank,
        shipped_date_day
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost ASC NULLS FIRST, id ASC) AS rn
        FROM base
    ) ranked
    WHERE rn = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY carrier ORDER BY shipped_date_day DESC, id ASC) AS shipped_date_day_bucket,
        RANK() OVER (PARTITION BY MONTH(shipped_date), carrier ORDER BY cost DESC) AS cost_rank,
        PERCENT_RANK() OVER (PARTITION BY carrier ORDER BY shipped_date_day DESC NULLS LAST) AS shipped_date_day_percent_rank
    FROM deduped
)
SELECT
    id,
    shipped_date,
    cost,
    status_prefix,
    latest_rank,
    shipped_date_day_bucket,
    cost_rank,
    shipped_date_day_percent_rank
FROM metrics
ORDER BY shipped_date_day_percent_rank DESC NULLS LAST;
