WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        LENGTH(TRIM(status)) AS status_len,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE end_date < DATE '2026-03-13'
      AND YEAR(start_date) = 2025
),
stats AS (
    -- Summarise per group
    SELECT
        holder_id,
        product,
        COUNT(*) AS row_count,
        MAX(status_len) AS max_status_len
    FROM cleaned
    GROUP BY holder_id, product
),
with_parent_holder AS (
    -- Enrich with dimension data
    SELECT
        s.holder_id,
        s.product,
        s.max_status_len,
        p.joined_on AS holder_joined_on,
        p.holder_name,
        p.region AS holder_region
    FROM stats s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE YEAR(p.joined_on) = 2024
),
enriched_holder AS (
    -- Join policyholders
    SELECT
        s.holder_id,
        s.product,
        s.max_status_len,
        s.holder_joined_on,
        s.holder_name,
        p.joined_on AS holder_joined_on_2
    FROM with_parent_holder s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY max_status_len, holder_id ORDER BY holder_joined_on, holder_id, product) AS holder_joined_on_quartile,
        MAX(max_status_len) OVER (PARTITION BY max_status_len, holder_id ORDER BY holder_joined_on, holder_id, product ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_max_status_len_so_far,
        COUNT(*) OVER (PARTITION BY MONTH(holder_joined_on_2)) AS rows_in_holder_joined_on_2_month,
        ROUND(PERCENT_RANK() OVER (PARTITION BY max_status_len, holder_id ORDER BY holder_joined_on, holder_id, product), 3) AS holder_joined_on_percent_rank
    FROM enriched_holder
    WINDOW w_max_status_len_holder_id_by AS (PARTITION BY max_status_len, holder_id ORDER BY holder_joined_on, holder_id, product)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        holder_id,
        product,
        holder_joined_on,
        holder_name,
        holder_joined_on_2,
        holder_joined_on_quartile,
        max_max_status_len_so_far,
        rows_in_holder_joined_on_2_month,
        holder_joined_on_percent_rank,
        LEAD(holder_joined_on_quartile, 3) OVER (PARTITION BY holder_id ORDER BY max_status_len, holder_id, product) AS next3_holder_joined_on_quartile,
        NTILE(4) OVER (PARTITION BY holder_id ORDER BY max_status_len, holder_id, product) AS max_status_len_quartile,
        FIRST_VALUE(holder_joined_on_2) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY max_status_len, holder_id, product ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_holder_joined_on_2,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id ORDER BY max_status_len), 3) AS max_status_len_cume_dist
    FROM with_trends
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        holder_id,
        product,
        holder_joined_on,
        holder_name,
        holder_joined_on_2,
        holder_joined_on_quartile,
        max_max_status_len_so_far,
        rows_in_holder_joined_on_2_month,
        holder_joined_on_percent_rank,
        next3_holder_joined_on_quartile,
        first_known_holder_joined_on_2,
        max_status_len_cume_dist,
        MIN(max_status_len_cume_dist) OVER w_holder_id AS holder_id_min_max_status_len_cume_dist,
        max_max_status_len_so_far - LAG(max_max_status_len_so_far) OVER (PARTITION BY holder_joined_on_quartile ORDER BY holder_joined_on_2, holder_id, product) AS max_max_status_len_so_far_change,
        MAX(holder_joined_on_quartile) OVER (PARTITION BY holder_id) AS holder_id_max_holder_joined_on_quartile,
        LAST_VALUE(max_status_len_cume_dist) IGNORE NULLS OVER (PARTITION BY holder_joined_on_quartile ORDER BY holder_joined_on_2, holder_id, product ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_max_status_len_cume_dist,
        ROUND(CUME_DIST() OVER w_product_max_status_len_quart, 3) AS first_known_holder_joined_on_2_cume_dist
    FROM with_windows
    WINDOW w_holder_id AS (PARTITION BY holder_id), w_product_max_status_len_quart AS (PARTITION BY product, max_status_len_quartile ORDER BY first_known_holder_joined_on_2 DESC)
)
SELECT
    holder_id,
    product,
    holder_name,
    holder_joined_on_2,
    holder_joined_on_quartile,
    rows_in_holder_joined_on_2_month,
    holder_joined_on_percent_rank,
    first_known_holder_joined_on_2,
    max_status_len_cume_dist,
    holder_id_min_max_status_len_cume_dist,
    max_max_status_len_so_far_change,
    holder_id_max_holder_joined_on_quartile,
    last_known_max_status_len_cume_dist
FROM ranked
ORDER BY rows_in_holder_joined_on_2_month DESC
