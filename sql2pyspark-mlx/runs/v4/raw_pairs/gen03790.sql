WITH cleaned AS (
    SELECT *, units * revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > DATE '2023-06-11'
),
scored AS (
    SELECT
        *,
        SUM(units) OVER (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units,
        CORR(revenue, units) OVER w_grp AS corr_revenue_units,
        NTILE(3) OVER (PARTITION BY store_id, product_id ORDER BY revenue_per_unit, id) AS revenue_per_unit_bucket,
        NTILE(10) OVER (PARTITION BY store_id, product_id ORDER BY revenue, id) AS revenue_bucket,
        NTILE(4) OVER w_part AS revenue_per_unit_bucket_2
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY product_id), w_part AS (ORDER BY revenue_per_unit DESC, id)
),
enriched AS (
    SELECT
        *,
        LEAD(revenue_per_unit_bucket_2, 2) OVER (ORDER BY revenue, id) AS next_revenue_per_unit_bucket_2,
        SUM(revenue_per_unit_bucket_2) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue_per_unit_bucket_2,
        CORR(revenue, units) OVER (PARTITION BY store_id, product_id) AS corr_revenue_units_2
    FROM scored
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
        running_units,
        corr_revenue_units,
        revenue_per_unit_bucket,
        revenue_bucket,
        revenue_per_unit_bucket_2,
        next_revenue_per_unit_bucket_2,
        cumulative_revenue_per_unit_bucket_2,
        corr_revenue_units_2,
        SUM(units) OVER (PARTITION BY store_id, product_id ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units,
        ROUND(running_units / SUM(running_units) OVER (PARTITION BY store_id, product_id), 4) AS running_units_share,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue_bucket, id) AS event_seq
    FROM enriched
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) AS rank_in_group
    FROM with_windows
    QUALIFY rank_in_group = 1
),
with_dims AS (
    SELECT
        s.*,
        p.cost,
        p.price,
        p.stock_qty
    FROM ranked s
    LEFT JOIN products p
        ON s.product_id = p.id
),
grouped AS (
    SELECT
        CASE WHEN revenue_per_unit_bucket_2 >= 7 THEN 'HIGH' ELSE 'LOW' END AS revenue_per_unit_bucket_2_tier,
        MIN(cumulative_units) AS min_cumulative_units,
        BIT_OR(units) AS units_bit_or,
        BIT_OR(units) AS units_bit_or_2,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(revenue_per_unit_bucket_2, 0.95) AS revenue_per_unit_bucket_2_p95
    FROM with_dims
    GROUP BY revenue_per_unit_bucket_2_tier
    HAVING BIT_OR(units) > 1
)
SELECT
    revenue_per_unit_bucket_2_tier,
    units_bit_or,
    units_bit_or_2,
    distinct_ids,
    revenue_per_unit_bucket_2_p95
FROM grouped
ORDER BY distinct_ids DESC
