WITH enriched AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.sale_date,
        s.units,
        s.revenue,
        p.region,
        p.store_name
    FROM store_sales s
    LEFT JOIN stores p
        ON s.store_id = p.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        sale_date,
        region
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue, id) = 1
),
enriched_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sale_date, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sale_date, id) AS row_seq
    FROM latest
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        region,
        event_seq,
        row_seq,
        LAG(event_seq, 1) OVER w_ordered AS prev_event_seq,
        COUNT(row_seq) OVER (PARTITION BY region) AS region_count_row_seq
    FROM enriched_store_sales
    WINDOW w_ordered AS (PARTITION BY region ORDER BY row_seq, id)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(region, 1, 3) AS region_prefix
    FROM scored
    WHERE region IS NOT NULL
),
scored_store_sales AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(prev_event_seq / SUM(prev_event_seq) OVER (PARTITION BY region), 4) AS prev_event_seq_share,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY region_count_row_seq, id) AS row_seq_2,
        MAX(region_count_row_seq) OVER (PARTITION BY region) AS region_max_region_count_row_seq
    FROM cleaned
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        region,
        SUM(prev_event_seq) AS total_prev_event_seq,
        COUNT(*) FILTER (WHERE region = 'West') AS west_rows,
        SUM(row_seq_2) FILTER (WHERE region = 'West') AS west_total_row_seq_2,
        PERCENTILE_APPROX(prev_event_seq, 0.95) AS prev_event_seq_p95
    FROM scored_store_sales
    GROUP BY region
    HAVING SUM(prev_event_seq) < 1000000
)
SELECT
    region,
    prev_event_seq_p95,
    CASE WHEN region RLIKE '^(Central|North)$' THEN 'CORE' WHEN region RLIKE '^(South|East)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS region_zone
FROM totals
