WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 1.5, 2) AS premium_adj
    FROM policies
    WHERE start_date < DATE '2025-07-07'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(premium_adj) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), product) AS start_date_month_product_max_premium_adj,
        NTH_VALUE(start_date, 2) OVER (ORDER BY premium_adj NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        SIZE(COLLECT_SET(holder_id) OVER w_start_date_month_product) AS distinct_holder_id_in_start_date_month_product,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium_adj NULLS LAST, id), 3) AS premium_adj_percent_rank,
        MAX(premium) OVER (ORDER BY premium_adj NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS premium_max_1_prior
    FROM filtered_policys
    WINDOW w_start_date_month_product AS (PARTITION BY DATE_TRUNC('MONTH', start_date), product)
),
stats AS (
    -- Summarise per group
    SELECT
        holder_id,
        MIN(premium_max_1_prior) AS min_premium_max_1_prior,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        MAX(distinct_holder_id_in_start_date_month_product) AS max_distinct_holder_id_in_start_date_month_produ
    FROM ranked
    GROUP BY 1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        min_premium_max_1_prior - LAG(min_premium_max_1_prior) OVER (PARTITION BY max_distinct_holder_id_in_start_date_month_produ ORDER BY max_distinct_holder_id_in_start_date_month_produ DESC, holder_id) AS min_premium_max_1_prior_change,
        LAST_VALUE(min_premium_max_1_prior) OVER (PARTITION BY max_distinct_holder_id_in_start_date_month_produ ORDER BY max_distinct_holder_id_in_start_date_month_produ DESC, holder_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_min_premium_max_1_prior,
        MIN(min_premium_max_1_prior) OVER (PARTITION BY max_distinct_holder_id_in_start_date_month_produ, holder_id) AS max_distinct_holder_id_in_start_date_month_produ_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY max_distinct_holder_id_in_start_date_month_produ ORDER BY max_distinct_holder_id_in_start_date_month_produ DESC), 3) AS max_distinct_holder_id_in_start_date_month_produ_3,
        COUNT(*) OVER (PARTITION BY max_distinct_holder_id_in_start_date_month_produ, holder_id) AS rows_in_max_distinct_holder_id_in_start_date_mon,
        FIRST_VALUE(min_premium_max_1_prior) OVER (PARTITION BY max_distinct_holder_id_in_start_date_month_produ ORDER BY max_distinct_holder_id_in_start_date_month_produ DESC, holder_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_min_premium_max_1_prior
    FROM stats
),
without_matches AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.product IN ('HOME', 'AUTO')
        )
)
SELECT
    holder_id,
    min_premium_max_1_prior,
    max_distinct_holder_id_in_start_date_month_produ,
    min_premium_max_1_prior_change,
    last_min_premium_max_1_prior,
    max_distinct_holder_id_in_start_date_month_produ_2,
    max_distinct_holder_id_in_start_date_month_produ_3
FROM without_matches
WHERE first_min_premium_max_1_prior BETWEEN 1680.0 AND 9820.0
ORDER BY min_premium_max_1_prior_change, last_min_premium_max_1_prior
