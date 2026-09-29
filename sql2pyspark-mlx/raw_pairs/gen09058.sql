WITH with_windows AS (
    SELECT
        *,
        AVG(revenue) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS revenue_avg_2_prior,
        NTILE(4) OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS revenue_bucket
    FROM store_sales
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY units DESC, id) AS dedup_rank
    FROM with_windows
    QUALIFY dedup_rank = 1
),
joined AS (
    SELECT
        s.id,
        s.store_id,
        s.sale_date,
        s.units,
        s.revenue,
        s.revenue_avg_2_prior,
        s.dedup_rank,
        p2.store_name,
        p2.region
    FROM ranked s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
),
remaining_keys AS (
    SELECT store_id, id
    FROM joined
    EXCEPT ALL
    SELECT store_id, id
    FROM store_sales
    WHERE units <> 20
),
verified AS (
    SELECT s.*
    FROM joined s
    JOIN remaining_keys k ON s.store_id = k.store_id AND s.id = k.id
),
scored AS (
    SELECT
        id,
        store_id,
        sale_date,
        revenue,
        revenue_avg_2_prior,
        dedup_rank,
        AVG(units) OVER w_running AS region_avg_units,
        SUM(revenue) OVER w_running AS region_sum_revenue
    FROM verified
    WHERE revenue_avg_2_prior <= 1855
    WINDOW w_running AS (PARTITION BY region)
)
SELECT
    id,
    sale_date,
    revenue,
    dedup_rank,
    region_sum_revenue
FROM scored
