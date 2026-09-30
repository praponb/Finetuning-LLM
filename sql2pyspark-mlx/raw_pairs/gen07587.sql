WITH cleaned AS (
    SELECT *, ROUND(quantity, 2) AS quantity_rounded
    FROM inventory
    WHERE MONTH(last_restock) IN (6, 9, 10)
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock NULLS FIRST, id) AS rn
    FROM cleaned
    QUALIFY rn = 1
),
with_dims AS (
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        a.quantity,
        a.last_restock,
        a.quantity_rounded,
        a.rn,
        b.launch_date,
        b.category,
        b.brand
    FROM ranked a
    LEFT JOIN products b
        ON a.product_id = b.id
),
ranked_inventory AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category, brand ORDER BY launch_date DESC, id) AS rank_in_group
        FROM with_dims
    ) ranked
    WHERE rank_in_group = 1
),
metrics AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity_rounded,
        launch_date,
        rank_in_group,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY launch_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('YEAR', launch_date) ORDER BY quantity_rounded DESC, id) AS quantity_rounded_bucket,
        LAG(quantity_rounded, 2, 0) OVER (ORDER BY last_restock, id) AS prev_quantity_rounded
    FROM ranked_inventory
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY prev_quantity_rounded NULLS FIRST, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY rank_in_group, id) AS rank_in_group_2
    FROM deduped
    QUALIFY rank_in_group_2 = 1
)
SELECT *
FROM (
    SELECT
        id,
        product_id,
        quantity_rounded,
        prev_quantity_rounded
    FROM latest
) src
UNPIVOT (
    value FOR metric_name IN (quantity_rounded, prev_quantity_rounded AS PREV_QUANTITY_ROUNDED)
);
