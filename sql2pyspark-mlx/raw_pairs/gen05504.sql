WITH base AS (
    SELECT *, COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE store_id IN (2, 3)
),
enriched AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity_filled,
        RANK() OVER (PARTITION BY product_id ORDER BY quantity_filled ASC NULLS LAST) AS quantity_filled_rank,
        LAST_VALUE(quantity_filled) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY quantity_filled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_filled,
        SUM(quantity_filled) OVER (PARTITION BY product_id ORDER BY quantity_filled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_filled
    FROM base
),
joined AS (
    SELECT
        src.*,
        p.price,
        p.cost,
        p.category
    FROM enriched src
    INNER JOIN products p
        ON src.product_id = p.id
    WHERE p.brand NOT IN ('Initech', 'Acme', 'Globex')
),
enriched_inventory AS (
    SELECT
        id,
        quantity_filled,
        last_known_quantity_filled,
        price,
        cost,
        LEAD(last_known_quantity_filled) OVER (PARTITION BY product_id ORDER BY quantity_filled ASC, id ASC) AS next_last_known_quantity_filled,
        LAG(last_known_quantity_filled) OVER (PARTITION BY product_id ORDER BY quantity_filled ASC, id ASC) AS prev_last_known_quantity_filled,
        LEAD(cost) OVER (PARTITION BY product_id ORDER BY quantity_filled ASC, id ASC) AS next_cost,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_filled ASC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY category, store_id ORDER BY last_known_quantity_filled DESC, id ASC) AS row_seq
    FROM joined
),
enriched_inventory_2 AS (
    SELECT
        id,
        quantity_filled,
        last_known_quantity_filled,
        price,
        prev_last_known_quantity_filled,
        next_cost,
        row_seq,
        NTILE(4) OVER (ORDER BY price ASC, id ASC) AS price_bucket,
        NTILE(3) OVER (ORDER BY next_last_known_quantity_filled ASC, id ASC) AS next_last_known_quantity_filled_bucket,
        AVG(quantity_filled) OVER (ORDER BY prev_last_known_quantity_filled ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS quantity_filled_avg_2
    FROM enriched_inventory
),
base_inventory AS (
    SELECT *, price * row_seq AS price_times_row_seq
    FROM enriched_inventory_2
    WHERE next_last_known_quantity_filled_bucket IS NOT NULL
)
SELECT
    CASE WHEN last_known_quantity_filled >= 76 THEN 'HIGH' ELSE 'LOW' END AS last_known_quantity_filled_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles
FROM base_inventory
GROUP BY last_known_quantity_filled_tier
ORDER BY last_known_quantity_filled_tier ASC NULLS FIRST
