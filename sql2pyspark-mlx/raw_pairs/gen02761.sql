WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(plan)) AS plan_title,
        DATEDIFF(start_date, end_date) / 7 AS weeks_end_date_to_start_date
    FROM subscriptions
    WHERE start_date > TO_DATE('2023-06-26')
      AND customer_id IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.plan,
        e.end_date,
        e.monthly_fee,
        e.status,
        e.plan_title,
        e.weeks_end_date_to_start_date,
        b.age
    FROM filtered e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status,
        plan_title,
        weeks_end_date_to_start_date,
        age,
        MAX(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_monthly_fee,
        COUNT(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_monthly_fee,
        MIN(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_monthly_fee
    FROM enriched
    WHERE customer_id = 32
),
filtered_subscriptions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        status,
        plan_title,
        age,
        last7_max_monthly_fee,
        last7_avg_monthly_fee,
        last7_count_monthly_fee,
        CASE WHEN last7_avg_monthly_fee >= 222 THEN 'High' WHEN last7_avg_monthly_fee >= 105 THEN 'Medium' ELSE 'Low' END AS last7_avg_monthly_fee_band
    FROM scored
    WHERE weeks_end_date_to_start_date <= 30
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY last7_count_monthly_fee DESC NULLS LAST, id ASC) AS dedup_rank
        FROM filtered_subscriptions
    ) x
    WHERE dedup_rank = 1
),
stats AS (
    -- Multi-level aggregation
    SELECT
        status,
        CASE WHEN age >= 61 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
        PERCENTILE_APPROX(last7_max_monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS last7_max_monthly_fee_quartiles,
        APPROX_PERCENTILE(last7_max_monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS last7_max_monthly_fee_quartiles_2,
        COUNT_IF(status = 'ACTIVE') AS active_events,
        COUNT(*) FILTER (WHERE status = 'EXPIRED') AS expired_rows,
        GROUPING(status) AS gid
    FROM ranked
    GROUP BY GROUPING SETS ((status, CASE WHEN age >= 61 THEN 'HIGH' ELSE 'LOW' END), (status))
    HAVING expired_rows >= 1
)
SELECT
    status,
    age_tier,
    last7_max_monthly_fee_quartiles,
    active_events,
    gid,
    CASE WHEN status RLIKE '^(CANCELLED|EXPIRED)$' THEN 'EXTENDED' WHEN status RLIKE '^(ACTIVE|PAUSED)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS status_zone
FROM stats
ORDER BY gid DESC;
