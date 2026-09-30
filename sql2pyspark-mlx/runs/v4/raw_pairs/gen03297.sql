WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM inventory
    WHERE last_restock >= DATE '2023-08-30'
),
enriched AS (
    -- Attach stores attributes
    SELECT
        a.*,
        s.city,
        s.sqft,
        s.region
    FROM filtered a
    LEFT JOIN stores s
        ON a.store_id = s.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY sqft DESC NULLS LAST) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
),
enriched_inventory AS (
    -- Attach stores attributes
    SELECT e.*, p.store_name
    FROM ranked e
    JOIN stores p
        ON e.store_id = p.id
)
-- Window metrics over several different partitions
SELECT
    *,
    AVG(quantity) OVER (ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_avg_3_prior,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY days_since_last_restock, id) AS row_seq,
    LAST_VALUE(sqft, TRUE) OVER (PARTITION BY product_id, city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft,
    PERCENT_RANK() OVER (PARTITION BY region, store_id ORDER BY sqft NULLS LAST) AS sqft_percent_rank,
    NTILE(3) OVER (PARTITION BY product_id ORDER BY sqft, id) AS sqft_bucket
FROM enriched_inventory;
