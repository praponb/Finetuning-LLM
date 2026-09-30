WITH base AS (
    SELECT
        *,
        FLOOR(rating) rating_whole,
        CASE WHEN city IN ('Berlin', 'Tokyo') THEN 'Core' ELSE 'Other' END city_group,
        COALESCE(city, 'N/A') resolved_city
    FROM suppliers
    WHERE rating < 3.0
),
scored AS (
    SELECT
        id,
        rating,
        rating_whole,
        city_group,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC NULLS FIRST) rating_dense_rank,
        NTILE(4) OVER (PARTITION BY city, country ORDER BY rating DESC, id) rating_bucket,
        SUM(rating) OVER (PARTITION BY city) city_sum_rating,
        LEAD(rating_whole, 1) OVER (PARTITION BY country ORDER BY rating, id) next_rating_whole
    FROM base
),
with_last AS (
    SELECT
        s.id,
        s.rating,
        s.rating_whole,
        latest.product_category,
        latest.product_cost
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.category AS product_category, c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    SELECT
        id,
        rating_whole,
        product_cost,
        AVG(rating) OVER (PARTITION BY product_category ORDER BY product_cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) rating_avg_6,
        PERCENT_RANK() OVER (PARTITION BY product_category ORDER BY rating_whole NULLS LAST) rating_whole_percent_rank
    FROM with_last
),
enriched AS (
    SELECT
        id,
        rating_whole,
        SUM(rating_avg_6) OVER w_grp cumulative_rating_avg_6,
        ROW_NUMBER() OVER w_hist event_seq,
        NTILE(5) OVER (ORDER BY rating_whole_percent_rank, id) rating_whole_percent_rank_bucket
    FROM with_windows
    WINDOW w_grp AS (ORDER BY rating_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY rating_whole, id)
),
scored_suppliers AS (
    SELECT
        *,
        SUM(event_seq) OVER (ORDER BY rating_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_event_seq,
        ROW_NUMBER() OVER (ORDER BY rating_whole, id) row_seq,
        LAST_VALUE(rating_whole) IGNORE NULLS OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_rating_whole
    FROM enriched
),
latest_match AS (
    SELECT s.*, top1.brand
    FROM scored_suppliers s
    JOIN LATERAL (
            SELECT c.brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    brand,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    COUNT(DISTINCT id) AS distinct_ids
FROM latest_match
GROUP BY 1
ORDER BY brand DESC NULLS LAST
