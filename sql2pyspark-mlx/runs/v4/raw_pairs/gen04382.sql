WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label,
        TRIM(region) AS clean_region
    FROM stores
    WHERE opened_date >= DATE '2023-08-27'
      AND opened_date < '2024-06-18'
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.revenue
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY city ORDER BY sqft ASC NULLS LAST) AS sqft_cume_dist,
        ROW_NUMBER() OVER w_part AS row_seq
    FROM with_latest
    WINDOW w_part AS (PARTITION BY city, region ORDER BY revenue ASC, id ASC)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY region, city ORDER BY sqft DESC) AS rn
        FROM metrics
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(revenue) OVER (PARTITION BY region ORDER BY revenue ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue,
        LAG(sqft, 1, 0) OVER (PARTITION BY region ORDER BY sqft_cume_dist ASC, id ASC) AS prev_sqft,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id ASC) AS event_seq,
        LEAD(sqft_cume_dist, 2, 0) OVER (ORDER BY rn ASC, id ASC) AS next_sqft_cume_dist,
        CUME_DIST() OVER (PARTITION BY city, region ORDER BY row_seq DESC) AS row_seq_cume_dist
    FROM deduped
),
metrics_stores AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        opened_date_label,
        clean_region,
        revenue,
        sqft_cume_dist,
        row_seq,
        rn,
        running_revenue,
        prev_sqft,
        event_seq,
        next_sqft_cume_dist,
        row_seq_cume_dist,
        LAG(sqft_cume_dist, 1) OVER (PARTITION BY region ORDER BY sqft_cume_dist ASC, id ASC) AS prev_sqft_cume_dist,
        SUM(next_sqft_cume_dist) OVER (ORDER BY sqft_cume_dist ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_sqft_cume_dist,
        SUM(sqft) OVER (ORDER BY running_revenue ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rn DESC, id ASC) AS row_seq_2,
        STDDEV_SAMP(row_seq_cume_dist) OVER (PARTITION BY region) AS region_stddev_samp_row_seq_cume_dist
    FROM with_windows
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.order_date >= s.opened_date
        )
)
SELECT
    id,
    city,
    opened_date,
    sqft,
    opened_date_label,
    clean_region,
    revenue,
    row_seq,
    running_revenue
FROM eligible
ORDER BY opened_date DESC
