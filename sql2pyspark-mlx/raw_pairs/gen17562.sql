WITH flattened AS (
    SELECT
        base.id,
        base.customer_id,
        base.coupon_codes,
        li_item.product_id,
        li_item.qty,
        li_item.price
    FROM carts base
    LATERAL VIEW OUTER EXPLODE(base.items) t AS li_item
),
joined AS (
    SELECT
        a.id,
        a.customer_id,
        a.coupon_codes,
        a.product_id,
        a.qty,
        d.country
    FROM flattened a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
with_windows AS (
    SELECT
        id,
        customer_id,
        RANK() OVER (ORDER BY qty DESC NULLS LAST) AS qty_rank,
        PERCENT_RANK() OVER (ORDER BY product_id DESC NULLS LAST) AS product_id_percent_rank,
        AVG(product_id) OVER w_running AS country_avg_product_id
    FROM joined
    WHERE customer_id IN (1, 3)
    WINDOW w_running AS (PARTITION BY country)
),
joined_carts AS (
    SELECT
        src.*,
        b.signup_date,
        b.segment,
        b.city
    FROM with_windows src
    INNER JOIN customers b
        ON src.customer_id = b.id
),
with_dims AS (
    SELECT src.*, p.city AS customer_city
    FROM joined_carts src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
enriched AS (
    SELECT
        a.id,
        a.customer_id,
        a.product_id_percent_rank,
        a.country_avg_product_id,
        a.segment,
        a.city,
        p.age,
        p.credit_limit
    FROM with_dims a
    LEFT JOIN customers p
        ON a.customer_id <=> p.id
),
with_windows_carts AS (
    SELECT
        *,
        STDDEV_SAMP(product_id_percent_rank) OVER (PARTITION BY segment, city) AS segment_stddev_samp_product_id_percent_rank,
        RANK() OVER w_hist AS age_rank,
        CUME_DIST() OVER w_ordered AS product_id_percent_rank_cume_dist
    FROM enriched
    WINDOW w_hist AS (PARTITION BY city ORDER BY age), w_ordered AS (PARTITION BY customer_id ORDER BY product_id_percent_rank DESC)
),
summary AS (
    SELECT
        CASE WHEN country_avg_product_id >= 11 THEN 'HIGH' ELSE 'LOW' END AS country_avg_product_id_tier,
        MAX(age) AS max_age,
        COUNT(*) FILTER (WHERE segment = 'Corporate') AS corporate_rows,
        ROUND(SUM(country_avg_product_id) / NULLIF(COUNT(CASE WHEN city = 'Berlin' THEN 1 END), 0), 2) AS country_avg_product_id_per_berlin,
        SUM(CASE WHEN city = 'Bangkok' THEN credit_limit ELSE 0 END) AS bangkok_credit_limit
    FROM with_windows_carts
    GROUP BY CASE WHEN country_avg_product_id >= 11 THEN 'HIGH' ELSE 'LOW' END
    HAVING MAX(age) > 0
)
SELECT
    country_avg_product_id_tier,
    corporate_rows,
    country_avg_product_id_per_berlin,
    bangkok_credit_limit
FROM summary
ORDER BY corporate_rows DESC;
