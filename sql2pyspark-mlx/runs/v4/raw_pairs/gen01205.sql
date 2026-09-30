WITH base AS (
    SELECT *, CEIL(rating) AS rating_whole
    FROM suppliers
    WHERE rating BETWEEN 2.3 AND 3.8
),
deduped AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id ASC) = 1
),
with_windows AS (
    SELECT
        *,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating_whole ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating,
        SUM(rating_whole) OVER w_grp AS running_rating_whole,
        AVG(rating_whole) OVER w_ordered AS rating_whole_avg_3,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating_whole ASC, id ASC) AS seq_no,
        LAG(rating, 1) OVER w_grp_2 AS prev_rating
    FROM deduped
    WINDOW w_grp AS (ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (ORDER BY rating_whole ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_grp_2 AS (PARTITION BY city ORDER BY rating ASC, id ASC)
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating ASC) AS row_num
        FROM with_windows
    ) t
    WHERE row_num = 1
),
with_latest AS (
    SELECT s.*, x.product_brand
    FROM ranked s
    JOIN LATERAL (
            SELECT c.brand AS product_brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    supplier_name,
    country,
    rating,
    rating_whole,
    running_rating_whole,
    rating_whole_avg_3,
    seq_no,
    product_brand
FROM with_latest
ORDER BY running_rating_whole ASC
