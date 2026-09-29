WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        rec.product_id,
        rec.qty,
        rec.price,
        rec.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) x AS rec
),
filtered AS (
    SELECT
        id,
        customer_id,
        created_at,
        product_id,
        price,
        CEIL(qty) AS qty_whole,
        qty - product_id AS qty_minus_product_id,
        COALESCE(promo, 'OTHER') AS resolved_promo
    FROM lines
    WHERE created_at > DATE '2025-03-01'
      AND customer_id IS NOT NULL
),
enriched AS (
    SELECT
        src.id,
        src.customer_id,
        src.product_id,
        src.resolved_promo,
        p.signup_date,
        p.credit_limit,
        p.age
    FROM filtered src
    LEFT JOIN customers p
        ON src.customer_id <=> p.id
    WHERE p.credit_limit BETWEEN 12600 AND 14900
),
metrics AS (
    SELECT
        resolved_promo,
        credit_limit,
        age,
        MAX(credit_limit) OVER w_running AS customer_id_max_credit_limit,
        COUNT(age) OVER w_running AS customer_id_count_age,
        PERCENT_RANK() OVER w_ordered AS product_id_percent_rank
    FROM enriched
    WINDOW w_running AS (PARTITION BY customer_id), w_ordered AS (PARTITION BY customer_id ORDER BY product_id)
),
valid AS (
    SELECT *, CAST(customer_id_max_credit_limit AS DOUBLE) AS customer_id_max_credit_limit_str
    FROM metrics
    WHERE product_id_percent_rank BETWEEN 0.6 AND 0.75
)
SELECT
    CASE WHEN customer_id_count_age >= 14 THEN 'PREMIUM' ELSE 'STANDARD' END AS customer_id_count_age_tier,
    COUNT(*) AS event_count,
    SUM(customer_id_max_credit_limit_str) AS total_customer_id_max_credit_limit_str,
    PERCENTILE_APPROX(customer_id_count_age, 0.5) AS customer_id_count_age_p50,
    PERCENTILE_APPROX(customer_id_max_credit_limit, ARRAY(0.25, 0.5, 0.75)) AS customer_id_max_credit_limit_quartiles
FROM valid
GROUP BY 1
