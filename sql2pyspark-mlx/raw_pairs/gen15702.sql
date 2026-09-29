WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        DATE_TRUNC('MONTH', shipped_date) AS shipped_date_period
    FROM shipments
    WHERE delivered_date >= DATE '2023-09-04'
      AND carrier IS DISTINCT FROM 'Flash'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        carrier,
        cost
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date NULLS LAST, id) = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT id, cost
    FROM (
        SELECT *, RANK() OVER (PARTITION BY carrier ORDER BY cost DESC) AS latest_rank
        FROM deduped
    ) x
    WHERE latest_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        cost,
        LEAD(cost, 2) OVER (ORDER BY cost, id) AS next_cost,
        NTILE(10) OVER w_ordered AS cost_bucket,
        ROW_NUMBER() OVER w_ordered AS seq_no
    FROM ranked
    WHERE cost BETWEEN 47 AND 83
    WINDOW w_ordered AS (ORDER BY cost DESC, id)
),
valid_shipments AS (
    -- Keep recent, valid rows
    SELECT
        id,
        cost,
        cost_bucket,
        seq_no,
        LEAST(seq_no, cost) AS least_seq_no_cost,
        GREATEST(cost, cost_bucket) AS greatest_cost_cost_bucket
    FROM metrics
    WHERE cost_bucket < 5
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN seq_no >= 9 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_tier,
        PERCENTILE_APPROX(greatest_cost_cost_bucket, ARRAY(0.25, 0.5, 0.75)) AS greatest_cost_cost_bucket_quartiles,
        COUNT(*) AS row_count
    FROM valid_shipments
    GROUP BY CASE WHEN seq_no >= 9 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    seq_no_tier,
    greatest_cost_cost_bucket_quartiles,
    row_count
FROM summary
ORDER BY seq_no_tier, row_count;
