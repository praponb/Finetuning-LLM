WITH matched_keys AS (
    SELECT order_id, id
    FROM shipments
    EXCEPT
    SELECT order_id, id
    FROM shipments
    WHERE YEAR(delivered_date) = 2025
),
reconciled AS (
    SELECT s.*
    FROM shipments s
    INNER JOIN matched_keys k ON s.order_id = k.order_id AND s.id = k.id
),
enriched AS (
    SELECT
        s.*,
        p.discount,
        p.amount,
        p.order_date
    FROM reconciled s
    INNER JOIN orders p
        ON s.order_id = p.id
),
scored AS (
    SELECT
        id,
        order_id,
        discount,
        order_date,
        STDDEV_SAMP(amount) OVER (PARTITION BY order_id, status) AS order_id_stddev_samp_amount
    FROM enriched
),
metrics AS (
    SELECT
        order_id,
        discount,
        order_id_stddev_samp_amount,
        RANK() OVER (PARTITION BY order_id ORDER BY order_id_stddev_samp_amount DESC NULLS LAST) AS order_id_stddev_samp_amount_rank
    FROM scored
),
valid AS (
    SELECT *, order_id_stddev_samp_amount * discount AS order_id_stddev_samp_amount_times_discount
    FROM metrics
    WHERE discount <= 0.24
)
SELECT *
FROM (
    SELECT
        order_id,
        discount,
        order_id_stddev_samp_amount_times_discount
    FROM valid
) src
UNPIVOT INCLUDE NULLS (
    reading FOR kpi IN (discount, order_id_stddev_samp_amount_times_discount AS ORDER_ID_STDDEV_SAMP_AMOUNT_TIMES_DISCOUNT_VAL)
)
