WITH cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        DATEDIFF(CURRENT_DATE(), sale_date) / 365 AS years_since_sale_date,
        revenue - units AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > DATE '2023-06-01'
),
ranked AS (
    SELECT *
    FROM cleaned
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY years_since_sale_date NULLS LAST) = 1
),
with_dims AS (
    SELECT
        a.*,
        p.cost,
        p.price
    FROM ranked a
    LEFT JOIN products p
        ON a.product_id = p.id
    WHERE SIZE(p.tags) > 2
),
kept_keys AS (
    SELECT id, product_id
    FROM with_dims
    INTERSECT
    SELECT id, product_id
    FROM store_sales
    WHERE units < 21
),
verified AS (
    SELECT s.*
    FROM with_dims s
    INNER JOIN kept_keys k ON s.id = k.id AND s.product_id = k.product_id
),
remaining_keys AS (
    SELECT product_id, store_id
    FROM verified
    INTERSECT
    SELECT product_id, store_id
    FROM store_sales
    WHERE MONTH(sale_date) IN (1, 4, 11)
),
reconciled AS (
    SELECT s.*
    FROM verified s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id AND s.store_id = k.store_id
)
SELECT
    CASE WHEN cost >= 740 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    store_id,
    AVG(units) AS avg_units,
    STDDEV_SAMP(revenue) AS revenue_stddev,
    CORR(units, revenue) AS corr_units_revenue
FROM reconciled
GROUP BY cost_tier, store_id
ORDER BY corr_units_revenue DESC NULLS LAST;
