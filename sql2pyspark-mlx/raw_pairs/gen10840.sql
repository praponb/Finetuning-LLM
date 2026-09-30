WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        units,
        revenue,
        CASE WHEN revenue >= 3900 THEN 'High' WHEN revenue >= 1700 THEN 'Medium' ELSE 'Low' END AS revenue_band
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-08-07')
      AND product_id IN (3, 7, 9)
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.revenue_band,
        p.brand,
        p.product_name
    FROM filtered src
    INNER JOIN products p
        ON src.product_id = p.id
    WHERE ARRAY_CONTAINS(p.tags, 'premium')
),
enriched AS (
    -- Attach products attributes
    SELECT a.*, b.category
    FROM with_dims a
    LEFT JOIN products b
        ON a.product_id = b.id
),
enriched_store_sales AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.revenue_band,
        s.brand,
        s.product_name,
        p.price,
        p.brand AS product_brand
    FROM enriched s
    LEFT JOIN products p
        ON s.product_id = p.id
),
enriched_store_sales_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        brand,
        ROW_NUMBER() OVER (ORDER BY price, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY product_brand ORDER BY price, id) AS seq_no,
        ROUND(price / SUM(price) OVER (PARTITION BY brand), 4) AS price_share
    FROM enriched_store_sales
),
enriched_store_sales_3 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY row_seq, id) AS event_seq,
        FIRST_VALUE(brand, TRUE) OVER (ORDER BY price_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_brand,
        NTILE(4) OVER (ORDER BY row_seq, id) AS row_seq_bucket
    FROM enriched_store_sales_2
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        brand,
        row_seq,
        seq_no,
        first_known_brand,
        row_seq_bucket
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY row_seq_bucket, id) AS dedup_rank
        FROM enriched_store_sales_3
    ) t
    WHERE dedup_rank = 1
)
SELECT
    id,
    seq_no,
    row_seq_bucket
FROM latest
ORDER BY row_seq_bucket DESC;
