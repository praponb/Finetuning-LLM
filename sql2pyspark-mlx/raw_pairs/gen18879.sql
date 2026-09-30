WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        sale_date,
        units,
        FLOOR(revenue) AS revenue_whole
    FROM store_sales
    WHERE sale_date > DATE '2023-05-06'
      AND units BETWEEN 26 AND 30
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        sale_date,
        revenue_whole,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) AS event_seq,
        LEAD(revenue_whole, 1, 0) OVER w_grp AS next_revenue_whole
    FROM filtered
    WHERE sale_date BETWEEN '2023-08-01' AND '2023-10-01'
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY units, id)
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.*,
        b.brand,
        b.product_name
    FROM scored e
    LEFT JOIN products b
        ON e.product_id = b.id
),
filtered_store_sales AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(event_seq, 0) AS event_seq_filled,
        DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_date_label
    FROM enriched
    WHERE sale_date > DATE '2023-05-24'
      AND brand IS NOT NULL
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.sale_date,
        src.event_seq,
        src.next_revenue_whole,
        src.event_seq_filled,
        src.sale_date_label,
        d.product_name AS product_product_name
    FROM filtered_store_sales src
    LEFT JOIN products d
        ON src.product_id = d.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY event_seq_filled, id) AS seq_no,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY next_revenue_whole DESC, id) AS next_revenue_whole_bucket,
        NTILE(3) OVER (PARTITION BY YEAR(sale_date) ORDER BY next_revenue_whole DESC, id) AS next_revenue_whole_bucket_2,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY event_seq_filled NULLS LAST) AS event_seq_filled_percent_rank,
        COUNT(event_seq_filled) OVER (PARTITION BY YEAR(sale_date), product_id) AS sale_date_year_count_event_seq_filled
    FROM with_dims
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY event_seq DESC NULLS LAST, id) AS rn
    FROM metrics
    QUALIFY rn = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        product_id,
        event_seq_filled,
        event_seq_filled_percent_rank
    FROM ranked
) src
UNPIVOT (
    metric_value FOR metric IN (event_seq_filled, event_seq_filled_percent_rank)
)
