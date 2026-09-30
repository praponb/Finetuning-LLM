WITH cleaned AS (
    SELECT *, CAST(quantity AS STRING) AS quantity_str
    FROM inventory
    WHERE quantity <> 110
),
scored AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        quantity_str,
        MAX(quantity) OVER w_recent AS running_max_quantity,
        MIN(quantity) OVER w_recent AS running_min_quantity,
        SUM(quantity) OVER w_recent AS running_sum_quantity,
        LEAD(quantity, 2) OVER w_running AS next_quantity
    FROM cleaned
    WHERE last_restock < '2023-05-01'
    WINDOW w_recent AS (PARTITION BY store_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY product_id ORDER BY quantity ASC, id ASC)
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity DESC NULLS LAST, id ASC) AS dedup_rank
        FROM scored
    ) t
    WHERE dedup_rank = 1
),
with_windows AS (
    SELECT
        *,
        LAST_VALUE(running_min_quantity, TRUE) OVER (ORDER BY running_sum_quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_min_quantity,
        NTILE(4) OVER (ORDER BY next_quantity DESC, id ASC) AS next_quantity_bucket,
        SUM(running_max_quantity) OVER w_ordered AS store_id_sum_running_max_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity ASC, id ASC) AS event_seq
    FROM deduped
    WINDOW w_ordered AS (PARTITION BY store_id)
),
enriched AS (
    SELECT
        src.*,
        s.opened_date,
        s.sqft,
        s.city
    FROM with_windows src
    LEFT JOIN stores s
        ON src.store_id = s.id
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, city ORDER BY running_max_quantity DESC, id ASC) AS rn
        FROM enriched
    ) x
    WHERE rn = 1
),
joined AS (
    SELECT
        s.id,
        s.product_id,
        s.quantity,
        s.quantity_str,
        s.running_sum_quantity,
        s.next_quantity,
        s.last_known_running_min_quantity,
        s.next_quantity_bucket,
        s.store_id_sum_running_max_quantity,
        s.sqft,
        s.rn,
        d.product_name,
        d.price,
        d.launch_date
    FROM ranked s
    INNER JOIN products d
        ON s.product_id = d.id
)
SELECT
    s.id,
    s.product_id,
    s.quantity,
    s.quantity_str,
    s.next_quantity,
    s.last_known_running_min_quantity,
    s.store_id_sum_running_max_quantity,
    s.sqft,
    s.rn,
    (SELECT MAX(o2.unit_price) FROM order_items o2 WHERE o2.product_id = s.product_id) AS max_order_item_unit_price
FROM joined s
ORDER BY max_order_item_unit_price DESC;
