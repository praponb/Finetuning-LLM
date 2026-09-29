WITH cleaned AS (
    SELECT *, units + revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date >= DATE '2023-03-23'
),
metrics AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY units DESC, id) AS units_bucket,
        LEAD(units, 2) OVER (PARTITION BY store_id ORDER BY units, id) AS next_units
    FROM cleaned
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) AS rn
    FROM metrics
    QUALIFY rn = 1
),
enriched AS (
    SELECT
        *,
        STDDEV_SAMP(units_bucket) OVER (PARTITION BY product_id) AS product_id_stddev_samp_units_bucket,
        ROW_NUMBER() OVER (ORDER BY rn DESC, id) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY units_bucket DESC, id) AS event_seq
    FROM ranked
),
joined AS (
    SELECT s.*, d.city
    FROM enriched s
    LEFT JOIN stores d
        ON s.store_id = d.id
),
with_windows AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_per_unit,
        units_bucket,
        next_units,
        rn,
        product_id_stddev_samp_units_bucket,
        row_seq,
        event_seq,
        city,
        RANK() OVER (PARTITION BY product_id, store_id ORDER BY row_seq DESC NULLS LAST) AS row_seq_rank,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY units_bucket DESC NULLS LAST) AS units_bucket_percent_rank
    FROM joined
),
with_dims AS (
    SELECT
        src.*,
        d.launch_date,
        d.price
    FROM with_windows src
    LEFT JOIN products d
        ON src.product_id = d.id
)
SELECT *
FROM (
    SELECT
        id,
        product_id,
        revenue,
        units_bucket_percent_rank,
        revenue_per_unit,
        price
    FROM with_dims
) src
UNPIVOT INCLUDE NULLS (
    reading FOR kpi IN (revenue AS REVENUE, units_bucket_percent_rank AS UNITS_BUCKET_PERCENT_RANK, revenue_per_unit AS REVENUE_PER_UNIT_VAL, price AS PRICE_VAL)
);
