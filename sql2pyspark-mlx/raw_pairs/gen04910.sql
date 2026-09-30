WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('-', category, product_name) AS category_product_name_key
    FROM products
    WHERE supplier_id IS NOT NULL
      AND ARRAY_CONTAINS(tags, 'clearance')
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand, supplier_id ORDER BY stock_qty DESC, id) AS dedup_rank
        FROM valid
    ) x
    WHERE dedup_rank = 1
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        s.*,
        p2.country,
        p2.supplier_name,
        p2.rating
    FROM deduped s
    LEFT JOIN suppliers p2
        ON s.supplier_id <=> p2.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        category,
        price,
        launch_date,
        tags,
        supplier_id,
        category_product_name_key,
        dedup_rank,
        country,
        AVG(rating) OVER (PARTITION BY country ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rating_avg_3,
        PERCENT_RANK() OVER (ORDER BY rating NULLS FIRST) AS rating_percent_rank
    FROM with_dims
    WHERE cost BETWEEN 590 AND 650
),
metrics_products AS (
    -- Running and moving aggregates
    SELECT
        id,
        category,
        launch_date,
        tags,
        supplier_id,
        dedup_rank,
        country,
        rating_avg_3,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date DESC, id) AS event_seq,
        AVG(rating_avg_3) OVER (PARTITION BY category, supplier_id ORDER BY rating_percent_rank, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS rating_avg_3_avg_5_prior,
        SUM(rating_avg_3) OVER (PARTITION BY supplier_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating_avg_3,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('YEAR', launch_date), category ORDER BY price, id) AS row_seq
    FROM metrics
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.category,
        s.launch_date,
        s.tags,
        s.supplier_id,
        s.country,
        s.rating_avg_3,
        s.event_seq,
        s.row_seq,
        top1.quantity
    FROM metrics_products s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Attach suppliers attributes
    SELECT s.*, d.supplier_name
    FROM with_latest s
    JOIN suppliers d
        ON s.supplier_id <=> d.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN row_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_tier,
    category,
    CASE WHEN rating_avg_3 >= 4 THEN 'LARGE' ELSE 'SMALL' END AS rating_avg_3_tier,
    APPROX_PERCENTILE(rating_avg_3, ARRAY(0.25, 0.5, 0.75)) AS rating_avg_3_quartiles,
    APPROX_PERCENTILE(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
    ROUND(AVG(row_seq), 2) AS avg_row_seq
FROM enriched
GROUP BY GROUPING SETS ((CASE WHEN row_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, category, CASE WHEN rating_avg_3 >= 4 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN row_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, category), ())
HAVING avg_row_seq > 0
