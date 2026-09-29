WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, unit_price + quantity AS line_total
    FROM order_items
    WHERE unit_price < 120
),
joined AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.brand,
        p.cost
    FROM cleaned src
    JOIN products p
        ON src.product_id = p.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS latest_rank
        FROM joined
    ) t
    WHERE latest_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(unit_price) OVER (PARTITION BY product_id, order_id) AS product_id_min_unit_price,
        SUM(quantity) OVER (PARTITION BY brand) AS brand_sum_quantity,
        AVG(unit_price) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS unit_price_avg_2
    FROM latest
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY latest_rank DESC, id) AS seq_no,
        FIRST_VALUE(cost) IGNORE NULLS OVER (ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost
    FROM metrics
),
cleaned_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(product_id_min_unit_price / first_known_cost, 2) AS product_id_min_unit_price_per_first_known_cost,
        CASE WHEN brand IN ('Stark', 'Initech') THEN 'Priority' ELSE 'Other' END AS brand_group,
        COALESCE(brand, 'OTHER') AS brand_filled
    FROM scored
    WHERE brand IS NOT NULL
      AND brand = 'Initech'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_total,
        brand,
        cost,
        latest_rank,
        product_id_min_unit_price,
        brand_sum_quantity,
        unit_price_avg_2,
        seq_no,
        first_known_cost,
        product_id_min_unit_price_per_first_known_cost,
        brand_group,
        brand_filled,
        COUNT(cost) OVER (PARTITION BY product_id) AS product_id_count_cost,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY first_known_cost NULLS FIRST) AS first_known_cost_dense_rank
    FROM cleaned_order_items
)
SELECT
    id,
    quantity,
    unit_price,
    line_total,
    brand,
    cost,
    latest_rank,
    product_id_min_unit_price,
    brand_sum_quantity,
    CASE WHEN brand RLIKE '^(Stark|Initech)$' THEN 'CORE' ELSE 'GROUP_B' END AS brand_zone
FROM with_windows;
