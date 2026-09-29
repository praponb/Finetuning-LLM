WITH valid AS (
    SELECT
        *,
        ROUND(quantity, 1) AS quantity_rounded,
        CASE WHEN quantity >= 252 THEN 'High' WHEN quantity >= 119 THEN 'Medium' ELSE 'Low' END AS quantity_level
    FROM inventory
    WHERE last_restock > DATE '2023-07-26'
      AND quantity != 145
),
with_dims AS (
    SELECT
        s.*,
        d.launch_date,
        d.price,
        d.product_name
    FROM valid s
    LEFT JOIN products d
        ON s.product_id = d.id
    WHERE SIZE(d.tags) >= 1
),
latest AS (
    SELECT
        id,
        quantity,
        last_restock,
        quantity_rounded,
        launch_date,
        price,
        product_name
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity_rounded DESC NULLS LAST, id) AS latest_rank
        FROM with_dims
    ) x
    WHERE latest_rank = 1
),
scored AS (
    SELECT
        *,
        NTILE(3) OVER (ORDER BY quantity DESC, id) AS quantity_bucket,
        NTILE(5) OVER (ORDER BY quantity_rounded DESC, id) AS quantity_rounded_bucket,
        PERCENT_RANK() OVER (ORDER BY price DESC NULLS FIRST) AS price_percent_rank,
        NTILE(3) OVER (ORDER BY quantity DESC, id) AS quantity_bucket_2,
        STDDEV_SAMP(price) OVER (PARTITION BY DATE_TRUNC('WEEK', launch_date)) AS launch_date_period_stddev_samp_price
    FROM latest
),
scored_inventory AS (
    SELECT
        id,
        quantity,
        last_restock,
        launch_date,
        price,
        quantity_rounded_bucket,
        quantity_bucket_2,
        launch_date_period_stddev_samp_price,
        FIRST_VALUE(quantity_bucket_2) IGNORE NULLS OVER (ORDER BY quantity_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_bucket_2,
        ROW_NUMBER() OVER (ORDER BY last_restock, id) AS seq_no,
        NTILE(10) OVER (ORDER BY quantity_rounded, id) AS quantity_rounded_bucket_2
    FROM scored
)
SELECT
    id,
    last_restock,
    launch_date,
    launch_date_period_stddev_samp_price
FROM scored_inventory;
