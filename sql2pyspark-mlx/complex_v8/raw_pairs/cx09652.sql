WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date
    FROM policies
    WHERE product = 'HOME'
),
with_parent_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        p.joined_on AS holder_joined_on
    FROM cleaned s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_joined_on,
        ROUND(premium / NULLIF(SUM(premium) OVER w_product, 0), 4) AS premium_share_of_product,
        ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY start_date DESC, id) AS seq_no,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY product)) AS distinct_holder_id_in_product,
        NTILE(4) OVER w_holder_joined_on_month_produ AS premium_quartile,
        premium - LAG(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on), product ORDER BY premium, id) AS premium_change,
        MAX(premium) OVER w_product AS product_max_premium,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on), product ORDER BY premium, id) AS premium_seq,
        DATEDIFF(start_date, LAG(start_date) OVER (ORDER BY holder_joined_on DESC, id)) AS days_since_prev_start_date,
        ROUND(CUME_DIST() OVER (ORDER BY holder_joined_on DESC), 3) AS holder_joined_on_cume_dist,
        MAX(premium) OVER (ORDER BY holder_joined_on DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS premium_max_7
    FROM with_parent_holder
    WINDOW w_product AS (PARTITION BY product), w_holder_joined_on_month_produ AS (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on), product ORDER BY premium, id)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY premium_seq, premium_quartile ORDER BY product_max_premium DESC, id) AS product_max_premium_rank,
        NTILE(5) OVER w_seq_no_by_distinct_holder_id AS distinct_holder_id_in_product_quintile,
        DENSE_RANK() OVER (PARTITION BY premium_quartile ORDER BY premium_change) AS premium_change_dense_rank,
        NTH_VALUE(premium_quartile, 2) OVER (PARTITION BY premium_seq, premium_quartile ORDER BY product_max_premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_quartile
    FROM with_trends
    WINDOW w_seq_no_by_distinct_holder_id AS (PARTITION BY seq_no ORDER BY distinct_holder_id_in_product ASC, id)
)
SELECT
    id,
    holder_joined_on,
    premium_share_of_product,
    distinct_holder_id_in_product,
    premium_quartile,
    rows_in_product,
    premium_seq,
    holder_joined_on_cume_dist,
    premium_max_7,
    product_max_premium_rank,
    distinct_holder_id_in_product_quintile,
    premium_change_dense_rank,
    second_premium_quartile,
    COALESCE(seq_no, 0) AS seq_no_filled
FROM ranked
WHERE premium_share_of_product < 0.8
ORDER BY id DESC, premium_max_7 DESC
