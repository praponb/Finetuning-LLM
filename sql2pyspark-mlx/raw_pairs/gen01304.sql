WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        start_date,
        status,
        CONCAT(status, ' - ', plan) AS status_plan
    FROM subscriptions
    WHERE monthly_fee >= 110
),
scored AS (
    -- Running and moving aggregates
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC, id) AS seq_no
    FROM cleaned
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(seq_no, 1) AS seq_no_rounded
    FROM scored
    WHERE start_date > DATE '2023-06-04'
),
base AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(plan) AS plan_length
    FROM filtered
    WHERE start_date >= DATE '2023-09-24'
)
SELECT
    id,
    start_date,
    status,
    seq_no,
    seq_no_rounded,
    plan_length
FROM base
ORDER BY seq_no, plan_length DESC;
