WITH enriched AS (
    SELECT
        src.*,
        p.category,
        p.brand,
        p.price
    FROM order_items src
    INNER JOIN products p
        ON src.product_id <=> p.id
),
enriched_order_items AS (
    SELECT
        id,
        order_id,
        unit_price,
        category,
        price,
        SUM(price) OVER (ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY quantity ASC, id ASC) AS quantity_bucket,
        PERCENT_RANK() OVER (PARTITION BY brand ORDER BY price DESC NULLS FIRST) AS price_percent_rank,
        LAG(price, 1) OVER w_ordered AS prev_price,
        AVG(unit_price) OVER (PARTITION BY category ORDER BY price ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_price_avg_6_prior
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY product_id, brand ORDER BY quantity ASC, id ASC)
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY category ORDER BY price_percent_rank ASC NULLS LAST) AS price_percent_rank_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY cumulative_price DESC, id ASC) AS event_seq,
        PERCENT_RANK() OVER (PARTITION BY order_id, category ORDER BY cumulative_price DESC NULLS FIRST) AS cumulative_price_percent_rank,
        LAG(quantity_bucket, 1) OVER (PARTITION BY order_id ORDER BY price_percent_rank ASC, id ASC) AS prev_quantity_bucket,
        ROUND(unit_price_avg_6_prior / SUM(unit_price_avg_6_prior) OVER (PARTITION BY category), 4) AS unit_price_avg_6_prior_share
    FROM enriched_order_items
),
scored_order_items AS (
    SELECT
        id,
        order_id,
        category,
        price,
        cumulative_price,
        quantity_bucket,
        prev_price,
        event_seq,
        prev_quantity_bucket,
        unit_price_avg_6_prior_share,
        MIN(price_percent_rank_percent_rank) OVER w_grp AS running_min_price_percent_rank_percent_rank,
        AVG(price_percent_rank_percent_rank) OVER w_grp AS running_avg_price_percent_rank_percent_rank,
        SUM(price_percent_rank_percent_rank) OVER w_grp AS running_sum_price_percent_rank_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY cumulative_price_percent_rank ASC, id ASC) AS row_seq
    FROM scored
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY unit_price_avg_6_prior_share ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    SELECT
        id,
        order_id,
        category,
        quantity_bucket,
        prev_price,
        unit_price_avg_6_prior_share,
        running_min_price_percent_rank_percent_rank,
        running_avg_price_percent_rank_percent_rank,
        row_seq
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cumulative_price DESC, id ASC) AS row_num
        FROM scored_order_items
    ) x
    WHERE row_num = 1
)
SELECT
    CASE WHEN running_min_price_percent_rank_percent_rank >= 0.67 THEN 'LARGE' ELSE 'SMALL' END AS running_min_price_percent_rank_percent_rank_tier,
    MAX(running_min_price_percent_rank_percent_rank) FILTER (WHERE category = 'Fashion') AS fashion_max_running_min_price_percent_rank_percent_rank,
    COUNT_IF(category = 'Electronics') AS electronics_events,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT order_id) AS distinct_order_ids
FROM latest
GROUP BY CASE WHEN running_min_price_percent_rank_percent_rank >= 0.67 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY distinct_order_ids ASC NULLS FIRST
