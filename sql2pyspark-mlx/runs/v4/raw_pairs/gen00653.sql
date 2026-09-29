WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id, supplier_id
    FROM products
    EXCEPT DISTINCT
    SELECT id, supplier_id
    FROM products
    WHERE stock_qty >= 193
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM products s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.supplier_id = k.supplier_id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.last_restock >= s.launch_date
        )
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        s.*,
        d.rating,
        d.supplier_name
    FROM not_flagged s
    LEFT JOIN suppliers d
        ON s.supplier_id = d.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER w_ordered AS rating_cume_dist,
        STDDEV_SAMP(rating) OVER (PARTITION BY category, supplier_id) AS category_stddev_samp_rating,
        DENSE_RANK() OVER (PARTITION BY supplier_id ORDER BY cost ASC NULLS FIRST) AS cost_dense_rank,
        RANK() OVER (PARTITION BY supplier_id ORDER BY rating DESC) AS rating_rank,
        DENSE_RANK() OVER w_recent AS cost_dense_rank_2
    FROM enriched
    WINDOW w_ordered AS (ORDER BY rating DESC NULLS FIRST), w_recent AS (ORDER BY cost ASC)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        rating,
        supplier_name,
        rating_cume_dist,
        category_stddev_samp_rating,
        cost_dense_rank,
        rating_rank,
        cost_dense_rank_2,
        CORR(price, stock_qty) OVER (PARTITION BY category, supplier_id) AS corr_price_stock_qty,
        SUM(rating_rank) OVER (PARTITION BY supplier_id) AS supplier_id_sum_rating_rank
    FROM scored
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date <= s.launch_date
        )
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            supplier_id,
            brand,
            cost
        FROM eligible
    ) src
    PIVOT (
        AVG(cost)
        FOR brand IN ('Globex', 'Initech')
    )
)
SELECT
    s.supplier_id,
    s.Globex,
    s.Initech,
    (SELECT MIN(p2.stock_qty) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS min_product_stock_qty
FROM pivoted s
