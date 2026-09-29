WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        DATE_ADD(end_date, 30) AS end_date_plus_30d
    FROM policies
    WHERE product IN ('LIFE', 'HOME')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY product, status ORDER BY end_date DESC NULLS FIRST), 3) AS end_date_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY product, status ORDER BY end_date DESC NULLS FIRST, id), 3) AS end_date_percent_rank_2,
        MIN(premium) OVER w_holder_id AS holder_id_min_premium,
        DATEDIFF(end_date_plus_30d, LAG(end_date_plus_30d) OVER w_product_status_by_end_date) AS days_since_prev_end_date_plus_30d
    FROM scoped
    WINDOW w_holder_id AS (PARTITION BY holder_id), w_product_status_by_end_date AS (PARTITION BY product, status ORDER BY end_date DESC NULLS FIRST, id)
),
screened AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.segment NOT IN ('RETAIL', 'VIP')
              AND p.joined_on IS NOT NULL
        )
)
SELECT
    product,
    status,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list,
    COUNT(*) AS n_rows
FROM screened
GROUP BY CUBE(product, status)
ORDER BY product NULLS LAST, status NULLS LAST
