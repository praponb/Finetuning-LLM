WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ADD_MONTHS(shipped_date, 6) AS shipped_date_plus_months,
        CONCAT_WS('/', carrier, status) AS carrier_status_key
    FROM shipments
    WHERE delivered_date >= DATE '2023-06-15'
      AND YEAR(shipped_date) = 2024
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        shipped_date,
        cost,
        MAX(cost) OVER w_running AS last3_max_cost,
        COUNT(cost) OVER w_running AS last3_count_cost,
        MIN(cost) OVER w_running AS last3_min_cost
    FROM valid
    WINDOW w_running AS (PARTITION BY status ORDER BY shipped_date_plus_months, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LAST_DAY(shipped_date) AS shipped_date_month_end
    FROM with_windows
    WHERE shipped_date > DATE '2023-03-13'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (ORDER BY last3_count_cost, id) AS last3_count_cost_bucket,
        LEAD(cost, 1, 0) OVER (ORDER BY last3_max_cost, id) AS next_cost,
        AVG(last3_max_cost) OVER (ORDER BY last3_min_cost, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS last3_max_cost_avg_6_prior,
        LAST_VALUE(last3_min_cost) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('month', shipped_date_month_end) ORDER BY last3_max_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last3_min_cost,
        ROW_NUMBER() OVER (ORDER BY last3_min_cost, id) AS row_seq
    FROM cleaned
),
cleaned_shipments AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(last3_count_cost, 0) AS last3_count_cost_rounded
    FROM enriched
    WHERE row_seq = 5
),
with_windows_shipments AS (
    -- Ranking and gap features
    SELECT
        id,
        shipped_date,
        last3_max_cost,
        last3_min_cost,
        last3_count_cost_bucket,
        last3_max_cost_avg_6_prior,
        row_seq,
        last3_count_cost_rounded,
        LEAD(last3_count_cost, 1) OVER (ORDER BY last3_max_cost, id) AS next_last3_count_cost,
        NTILE(5) OVER (ORDER BY last_known_last3_min_cost DESC, id) AS last_known_last3_min_cost_bucket
    FROM cleaned_shipments
    WHERE last3_max_cost BETWEEN 54 AND 91
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', shipped_date) AS week_start,
    PERCENTILE_APPROX(last3_min_cost, 0.95) AS last3_min_cost_p95,
    COUNT(*) AS num_rows,
    ROUND(AVG(last3_max_cost), 2) AS avg_last3_max_cost
FROM with_windows_shipments
GROUP BY DATE_TRUNC('WEEK', shipped_date)
ORDER BY num_rows DESC NULLS FIRST;
