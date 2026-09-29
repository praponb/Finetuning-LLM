WITH scored AS (
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        SUM(unit_price) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price,
        STDDEV_SAMP(unit_price) OVER (PARTITION BY order_id) AS order_id_stddev_samp_unit_price,
        ROW_NUMBER() OVER (ORDER BY unit_price DESC, id) AS event_seq,
        NTILE(4) OVER (PARTITION BY product_id, order_id ORDER BY unit_price, id) AS unit_price_bucket,
        CORR(unit_price, quantity) OVER (PARTITION BY order_id) AS corr_unit_price_quantity
    FROM order_items
    WHERE order_id = 40
),
joined AS (
    SELECT a.*, d.order_date
    FROM scored a
    LEFT JOIN orders d
        ON a.order_id = d.id
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price_bucket DESC NULLS FIRST, id) AS latest_rank
    FROM joined
    QUALIFY latest_rank = 1
),
ranked AS (
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price_bucket, id) = 1
),
scored_order_items AS (
    SELECT
        *,
        NTILE(3) OVER w_running AS order_id_stddev_samp_unit_price_bucket,
        DENSE_RANK() OVER w_grp AS cumulative_unit_price_dense_rank
    FROM ranked
    WINDOW w_running AS (PARTITION BY MONTH(order_date), order_id ORDER BY order_id_stddev_samp_unit_price, id), w_grp AS (PARTITION BY order_id ORDER BY cumulative_unit_price DESC)
),
stats AS (
    SELECT
        DATE_TRUNC('YEAR', order_date) AS year_start,
        DATE_TRUNC('MONTH', order_date) AS activity_month,
        COUNT(DISTINCT order_id) AS distinct_order_ids,
        BIT_AND(quantity) AS quantity_bit_and,
        APPROX_PERCENTILE(order_id_stddev_samp_unit_price_bucket, 0.9) AS order_id_stddev_samp_unit_price_bucket_p90
    FROM scored_order_items
    GROUP BY CUBE (DATE_TRUNC('YEAR', order_date), DATE_TRUNC('MONTH', order_date))
    HAVING order_id_stddev_samp_unit_price_bucket_p90 < 1000000
)
SELECT
    year_start,
    activity_month,
    distinct_order_ids,
    quantity_bit_and,
    order_id_stddev_samp_unit_price_bucket_p90
FROM stats
ORDER BY order_id_stddev_samp_unit_price_bucket_p90 DESC NULLS LAST;
