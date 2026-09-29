WITH base AS (
    SELECT
        *,
        LOWER(city) AS city_lower,
        CONCAT(city, ' - ', country) AS city_country
    FROM suppliers
    WHERE city IN ('Singapore', 'Tokyo')
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS event_seq,
        NTILE(10) OVER (ORDER BY rating DESC, id) AS rating_bucket
    FROM base
),
with_latest AS (
    SELECT
        s.*,
        top1.launch_date,
        top1.stock_qty
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.launch_date, c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics_suppliers AS (
    SELECT
        *,
        ROUND(rating_bucket / SUM(rating_bucket) OVER (PARTITION BY country), 4) AS rating_bucket_share,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY MONTH(launch_date), city), 4) AS event_seq_share,
        LEAD(stock_qty, 1) OVER (PARTITION BY country ORDER BY event_seq, id) AS next_stock_qty,
        NTILE(5) OVER (PARTITION BY MONTH(launch_date), city ORDER BY rating_bucket, id) AS rating_bucket_bucket
    FROM with_latest
),
filtered AS (
    SELECT
        *,
        event_seq_share + rating_bucket_share AS event_seq_share_plus_rating_bucket_share,
        ROUND(next_stock_qty, 1) AS next_stock_qty_rounded,
        COALESCE(city, 'N/A') AS resolved_city
    FROM metrics_suppliers
    WHERE city IS NOT NULL
      AND rating_bucket_bucket != 7
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating_bucket_share DESC, id) AS rn
    FROM filtered
    QUALIFY rn = 1
)
SELECT
    city,
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    country,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    PERCENTILE_APPROX(next_stock_qty, 0.9) AS next_stock_qty_p90,
    COUNT(*) FILTER (WHERE country = 'UK') AS uk_rows
FROM deduped
GROUP BY GROUPING SETS ((city, DATE_TRUNC('YEAR', launch_date), country), (city, DATE_TRUNC('YEAR', launch_date)));
