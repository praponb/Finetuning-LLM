WITH cleaned AS (
    SELECT
        id,
        product_id,
        ROUND(quantity * 1.15, 2) AS quantity_scaled,
        COALESCE(quantity, 0) AS quantity_filled
    FROM order_items
    WHERE product_id IS NOT NULL
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY quantity_scaled DESC NULLS LAST) AS dedup_rank
        FROM cleaned
    ) t
    WHERE dedup_rank = 1
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_scaled DESC, id) AS event_seq,
        FIRST_VALUE(quantity_scaled, TRUE) OVER (PARTITION BY product_id ORDER BY quantity_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_scaled,
        PERCENT_RANK() OVER (ORDER BY quantity_filled) AS quantity_filled_percent_rank
    FROM deduped
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_filled_percent_rank, id) AS event_seq_2,
        AVG(quantity_filled_percent_rank) OVER w_hist AS product_id_avg_quantity_filled_percent_rank,
        ROW_NUMBER() OVER w_recent AS event_seq_3
    FROM enriched
    WINDOW w_hist AS (PARTITION BY product_id), w_recent AS (PARTITION BY product_id ORDER BY dedup_rank DESC, id)
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_scaled NULLS FIRST, id) AS rn
        FROM scored
    ) t
    WHERE rn = 1
),
valid AS (
    SELECT *, quantity_filled_percent_rank * event_seq_2 AS quantity_filled_percent_rank_times_event_seq_2
    FROM latest
    WHERE dedup_rank = 1
),
with_dims AS (
    SELECT
        s.*,
        d.category,
        d.brand,
        d.stock_qty
    FROM valid s
    INNER JOIN products d
        ON s.product_id = d.id
)
SELECT
    *,
    AVG(stock_qty) OVER (ORDER BY rn, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS stock_qty_avg_2_prior,
    NTILE(3) OVER (PARTITION BY product_id ORDER BY first_known_quantity_scaled, id) AS first_known_quantity_scaled_bucket
FROM with_dims;
