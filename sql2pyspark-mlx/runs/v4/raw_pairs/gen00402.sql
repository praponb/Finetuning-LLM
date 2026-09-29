WITH remaining_keys AS (
    SELECT product_id
    FROM order_items
    EXCEPT ALL
    SELECT product_id
    FROM order_items
    WHERE product_id = 4
),
kept_rows AS (
    SELECT s.*
    FROM order_items s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id
),
cleaned AS (
    SELECT
        *,
        quantity + unit_price line_total,
        CASE WHEN unit_price >= 390 THEN 'High' WHEN unit_price >= 130 THEN 'Medium' ELSE 'Low' END unit_price_level
    FROM kept_rows
    WHERE product_id IS NOT NULL
),
enriched AS (
    SELECT
        s.*,
        b.price,
        b.category,
        b.brand
    FROM cleaned s
    LEFT JOIN products b
        ON s.product_id = b.id
),
filtered AS (
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        line_total,
        unit_price_level,
        brand,
        quantity + unit_price line_total_2,
        CEIL(price) price_whole,
        COALESCE(category, 'OTHER') category_clean
    FROM enriched
    WHERE product_id IS NOT NULL
),
enriched_order_items AS (
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        line_total,
        unit_price_level,
        brand,
        line_total_2,
        price_whole,
        category_clean,
        PERCENT_RANK() OVER w_grp price_whole_percent_rank,
        PERCENT_RANK() OVER w_hist line_total_percent_rank,
        PERCENT_RANK() OVER w_grp_2 price_whole_percent_rank_2
    FROM filtered
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY price_whole ASC), w_hist AS (ORDER BY line_total DESC), w_grp_2 AS (ORDER BY price_whole DESC NULLS LAST)
),
enriched_order_items_2 AS (
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        line_total,
        unit_price_level,
        brand,
        line_total_2,
        price_whole,
        category_clean,
        price_whole_percent_rank,
        line_total_percent_rank,
        price_whole_percent_rank_2,
        PERCENT_RANK() OVER w_part price_whole_percent_rank_3,
        AVG(line_total_percent_rank) OVER w_grp order_id_avg_line_total_percent_rank,
        CUME_DIST() OVER (ORDER BY price_whole_percent_rank DESC) price_whole_percent_rank_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY line_total DESC) line_total_percent_rank_2
    FROM enriched_order_items
    WINDOW w_part AS (ORDER BY price_whole ASC NULLS FIRST), w_grp AS (PARTITION BY order_id, product_id)
)
SELECT
    id,
    order_id,
    product_id,
    line_total,
    unit_price_level,
    price_whole,
    category_clean,
    price_whole_percent_rank,
    line_total_percent_rank
FROM enriched_order_items_2
ORDER BY line_total ASC, price_whole_percent_rank ASC;
