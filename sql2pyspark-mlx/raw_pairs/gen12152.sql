WITH valid AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        DATEDIFF(CURRENT_DATE(), shipped_date) / 365.25 AS years_since_shipped_date,
        COALESCE(carrier, 'NONE') AS carrier_clean
    FROM shipments
    WHERE order_id = 1
),
with_windows AS (
    SELECT
        *,
        SUM(years_since_shipped_date) OVER w_grp AS last7_sum_years_since_shipped_date,
        AVG(years_since_shipped_date) OVER w_grp AS last7_avg_years_since_shipped_date,
        MAX(years_since_shipped_date) OVER w_grp AS last7_max_years_since_shipped_date,
        COUNT(years_since_shipped_date) OVER w_grp AS last7_count_years_since_shipped_date
    FROM valid
    WINDOW w_grp AS (PARTITION BY carrier ORDER BY years_since_shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
with_dims AS (
    SELECT
        e.*,
        d.channel,
        d.discount,
        d.status
    FROM with_windows e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
metrics AS (
    SELECT
        id,
        order_id,
        carrier,
        last7_avg_years_since_shipped_date,
        discount,
        LEAD(last7_avg_years_since_shipped_date, 1) OVER (PARTITION BY MONTH(delivered_date), order_id ORDER BY last7_sum_years_since_shipped_date, id) AS next_last7_avg_years_since_shipped_date,
        FIRST_VALUE(carrier) IGNORE NULLS OVER (ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_carrier
    FROM with_dims
    WHERE years_since_shipped_date < 74
),
with_windows_shipments AS (
    SELECT
        *,
        COUNT(next_last7_avg_years_since_shipped_date) OVER (PARTITION BY carrier ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_next_last7_avg_years_since_shipped_date,
        MAX(next_last7_avg_years_since_shipped_date) OVER (PARTITION BY carrier ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_next_last7_avg_years_since_shipped_date,
        SUM(next_last7_avg_years_since_shipped_date) OVER (PARTITION BY carrier ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_next_last7_avg_years_since_shipped_date
    FROM metrics
)
SELECT
    p.status,
    p.channel,
    COUNT(*) AS rows_total,
    ROUND(AVG(s.running_count_next_last7_avg_years_since_shipped_date), 2) AS avg_running_count_next_last7_avg_years_since_shipped_date
FROM with_windows_shipments s
INNER JOIN orders p ON s.order_id = p.id
GROUP BY p.status, p.channel
ORDER BY status DESC NULLS FIRST
