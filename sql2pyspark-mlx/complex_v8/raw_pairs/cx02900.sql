WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        CASE WHEN status IN ('LAPSED', 'EXPIRED') THEN 'group_a' WHEN status = 'ACTIVE' THEN 'group_b' ELSE 'other' END AS status_group,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix
    FROM policies
    WHERE holder_id IS NOT NULL
      AND premium > 2111.3
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status_or_unknown,
        premium - LAG(premium) OVER (PARTITION BY status_or_unknown ORDER BY start_date DESC, id) AS premium_change,
        ROUND(premium - AVG(premium) OVER w_holder_id, 2) AS premium_vs_holder_id_avg,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id), 2) AS holder_id_avg_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER w_holder_id, 0), 4) AS premium_share_of_holder_id
    FROM filtered
    WINDOW w_holder_id AS (PARTITION BY holder_id)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        product,
        premium,
        start_date,
        end_date,
        status_or_unknown,
        holder_id_avg_premium,
        premium_share_of_holder_id,
        MIN(premium_share_of_holder_id) OVER (PARTITION BY status_or_unknown) AS status_or_unknown_min_premium_share_of_holder_id,
        ROUND(AVG(premium_change) OVER (PARTITION BY MONTH(end_date) ORDER BY premium DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS premium_change_avg_2_prior
    FROM sequenced
)
SELECT
    COALESCE(product, 'ALL') AS product,
    COALESCE(status_or_unknown, 'ALL') AS status_or_unknown,
    GROUPING(product) AS is_total_product,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list,
    COUNT(*) AS cnt
FROM ranked
GROUP BY GROUPING SETS ((product, status_or_unknown), (product), (status_or_unknown))
ORDER BY product NULLS LAST, status_or_unknown NULLS LAST
