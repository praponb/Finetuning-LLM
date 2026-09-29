WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY discount DESC NULLS FIRST, id) AS row_num
        FROM orders
    ) x
    WHERE row_num = 1
),
attributed AS (
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM ranked s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.order_date >= pr.starts_on
       AND s.order_date < pr.ends_on + INTERVAL 30 DAYS
),
metrics AS (
    SELECT
        id,
        customer_id,
        order_date,
        amount,
        discount,
        row_num,
        promo_id,
        promo_code,
        discount_pct,
        SUM(discount_pct) OVER (PARTITION BY status, promo_code ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct,
        RANK() OVER (PARTITION BY YEAR(order_date), promo_code ORDER BY amount) AS amount_rank,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY discount, id, promo_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_2_prior
    FROM attributed
),
cleaned AS (
    SELECT
        id,
        customer_id,
        order_date,
        amount,
        discount,
        row_num,
        promo_id,
        promo_code,
        discount_pct,
        amount_rank,
        discount_pct_avg_2_prior,
        CASE WHEN discount_pct >= 0.28 THEN 'High' WHEN discount_pct >= 0.1 THEN 'Medium' ELSE 'Low' END AS discount_pct_level,
        COALESCE(promo_code, 'NONE') AS resolved_promo_code
    FROM metrics
    WHERE order_date >= DATE '2023-03-21'
      AND promo_code IS DISTINCT FROM 'BUNDLE'
)
SELECT
    promo_code,
    promo_id,
    PERCENTILE_APPROX(discount_pct_avg_2_prior, 0.9) AS discount_pct_avg_2_prior_p90,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    SUM(CASE WHEN promo_code = 'BUNDLE' THEN amount_rank ELSE 0 END) AS bundle_amount_rank
FROM cleaned
GROUP BY promo_code, promo_id
HAVING SUM(CASE WHEN promo_code = 'BUNDLE' THEN amount_rank ELSE 0 END) > 0
ORDER BY distinct_customer_ids DESC NULLS LAST;
