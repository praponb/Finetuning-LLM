WITH base AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        CAST(quantity AS STRING) quantity_str,
        FLOOR(quantity) quantity_whole
    FROM inventory
    WHERE last_restock > DATE '2023-09-07'
      AND last_restock <= '2024-02-01'
),
joined AS (
    SELECT
        src.id,
        src.store_id,
        src.quantity,
        src.last_restock,
        src.quantity_str,
        s.store_name
    FROM base src
    LEFT JOIN stores s
        ON src.store_id = s.id
    WHERE s.sqft BETWEEN 8512 AND 15260
),
scored AS (
    SELECT
        id,
        last_restock,
        quantity_str,
        store_name,
        NTILE(5) OVER (PARTITION BY store_id ORDER BY quantity DESC, id) quantity_bucket,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock, id) row_seq
    FROM joined
),
valid AS (
    SELECT
        id,
        last_restock,
        quantity_str,
        quantity_bucket,
        row_seq,
        CEIL(row_seq) row_seq_whole
    FROM scored
    WHERE UPPER(store_name) LIKE '%IRP%'
),
valid_inventory AS (
    SELECT
        id,
        last_restock,
        quantity_str,
        row_seq,
        row_seq_whole,
        quantity_bucket * row_seq quantity_bucket_times_row_seq
    FROM valid
    WHERE last_restock > DATE '2023-04-29'
)
SELECT
    id,
    last_restock,
    quantity_str,
    row_seq,
    row_seq_whole,
    quantity_bucket_times_row_seq
FROM valid_inventory;
