WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        base.id,
        base.customer_id,
        base.created_at,
        base.coupon_codes,
        elem.product_id,
        elem.qty,
        elem.promo
    FROM carts base
    LATERAL VIEW EXPLODE(base.items) li AS elem
),
exploded_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        product_id,
        qty,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM exploded
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.signup_date,
        b.name,
        b.city
    FROM exploded_carts e
    LEFT JOIN customers b
        ON e.customer_id = b.id
    WHERE b.segment IN ('Corporate', 'SMB', 'Consumer')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(qty) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS qty_avg_1h,
        MIN(product_id) OVER w_hist AS customer_id_min_product_id,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY element), 4) AS product_id_share
    FROM joined
    WINDOW w_hist AS (PARTITION BY customer_id)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('WEEK', signup_date) AS signup_date_period,
        COALESCE(element, 'DIRECT') AS resolved_element
    FROM enriched
    WHERE customer_id_min_product_id > 9
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        product_id,
        element,
        signup_date,
        city,
        product_id_share,
        resolved_element,
        DENSE_RANK() OVER w_grp AS qty_dense_rank,
        STDDEV_SAMP(product_id) OVER (PARTITION BY TO_DATE(created_at), city) AS created_at_to_date_stddev_samp_product_id,
        SUM(qty) OVER (PARTITION BY element, city) AS element_sum_qty,
        COUNT(product_id_share) OVER (PARTITION BY element ORDER BY CAST(signup_date_period AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS product_id_share_count_90d,
        CUME_DIST() OVER (PARTITION BY element, city ORDER BY product_id_share DESC) AS product_id_share_cume_dist
    FROM filtered
    WINDOW w_grp AS (PARTITION BY TO_DATE(created_at), customer_id ORDER BY qty ASC)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        FLOOR(product_id_share_cume_dist) AS product_id_share_cume_dist_whole,
        ROUND(product_id_share / product_id, 2) AS product_id_share_per_product_id,
        COALESCE(element, 'DIRECT') AS element_clean
    FROM with_windows
    WHERE city IS NOT NULL
      AND city = 'Bangkok'
)
SELECT
    id,
    customer_id,
    product_id,
    element,
    product_id_share,
    resolved_element,
    qty_dense_rank,
    created_at_to_date_stddev_samp_product_id,
    product_id_share_count_90d,
    CASE WHEN element RLIKE '^(BUNDLE|WELCOME5)$' THEN 'GROUP_B' WHEN element RLIKE '^(SPRING10|VIP)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS element_zone
FROM cleaned
