WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        delivered_date,
        cost,
        status,
        INITCAP(TRIM(status)) AS status_title
    FROM shipments
    WHERE cost >= 74
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        cost,
        status,
        status_title,
        CAST(cost AS INT) AS cost_int,
        LOWER(status) AS status_lower
    FROM cleaned
    WHERE delivered_date > TO_DATE('2023-08-14')
      AND order_id IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) AS cost_share,
        NTILE(3) OVER (PARTITION BY status ORDER BY cost_int, id) AS cost_int_bucket,
        AVG(cost) OVER (PARTITION BY status ORDER BY cost_int, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS cost_avg_5_prior
    FROM filtered
    WHERE cost_int IS NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        cost,
        status,
        cost_share,
        cost_int_bucket
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC, id) = 1
)
SELECT
    id,
    cost,
    status,
    CASE WHEN status RLIKE '^(IN_TRANSIT|RETURNED)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS status_zone
FROM latest
ORDER BY cost, status DESC
