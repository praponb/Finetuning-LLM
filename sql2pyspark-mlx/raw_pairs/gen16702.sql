WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity NULLS FIRST, id) AS row_num
        FROM inventory
    ) t
    WHERE row_num = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        row_num,
        DENSE_RANK() OVER w_running AS quantity_dense_rank,
        PERCENT_RANK() OVER w_recent AS quantity_percent_rank,
        NTILE(10) OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS quantity_bucket
    FROM deduped
    WINDOW w_running AS (PARTITION BY YEAR(last_restock), product_id ORDER BY quantity NULLS LAST), w_recent AS (PARTITION BY store_id ORDER BY quantity DESC NULLS LAST)
),
deduped_inventory AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY last_restock) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(quantity, TRUE) OVER (PARTITION BY product_id, store_id ORDER BY quantity_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
        NTILE(5) OVER (PARTITION BY MONTH(last_restock), product_id ORDER BY quantity_dense_rank DESC, id) AS quantity_dense_rank_bucket,
        ROW_NUMBER() OVER (ORDER BY quantity_bucket, id) AS seq_no,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_avg_3_prior
    FROM deduped_inventory
),
joined AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.quantity,
        s.last_restock,
        s.quantity_percent_rank,
        s.quantity_bucket,
        s.latest_rank,
        s.first_known_quantity,
        s.seq_no,
        s.quantity_avg_3_prior,
        p.region,
        p.store_name
    FROM scored s
    LEFT JOIN stores p
        ON s.store_id = p.id
)
SELECT
    id,
    last_restock,
    quantity_percent_rank,
    latest_rank,
    first_known_quantity,
    seq_no
FROM joined
ORDER BY seq_no DESC, quantity_percent_rank DESC
