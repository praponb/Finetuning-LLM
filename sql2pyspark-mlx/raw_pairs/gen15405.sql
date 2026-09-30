WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        TRIM(country) AS clean_country,
        LOWER(supplier_name) AS supplier_name_lower
    FROM suppliers
    WHERE city IS NOT NULL
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.clean_country,
        s.supplier_name_lower,
        top1.product_cost,
        top1.product_price
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.cost AS product_cost, c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name_lower,
        product_cost,
        product_price,
        LEAD(product_cost, 1) OVER (PARTITION BY city ORDER BY product_cost, id) AS next_product_cost,
        SUM(product_price) OVER (PARTITION BY city ORDER BY product_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_product_price,
        AVG(product_price) OVER (PARTITION BY city ORDER BY product_cost, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS product_price_avg_3_prior
    FROM latest_match
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(product_price_avg_3_prior, TRUE) OVER (ORDER BY product_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_product_price_avg_3_prior,
        AVG(product_price) OVER (ORDER BY product_cost, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS product_price_avg_3_prior_2,
        LAG(product_cost, 2) OVER (ORDER BY product_cost, id) AS prev_product_cost,
        LAST_VALUE(product_price_avg_3_prior) IGNORE NULLS OVER (ORDER BY product_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_product_price_avg_3_prior_2,
        AVG(cumulative_product_price) OVER (ORDER BY product_price_avg_3_prior, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cumulative_product_price_avg_2_prior
    FROM enriched
),
with_windows_suppliers AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name_lower,
        product_cost,
        product_price,
        next_product_cost,
        cumulative_product_price,
        last_known_product_price_avg_3_prior,
        prev_product_cost,
        last_known_product_price_avg_3_prior_2,
        cumulative_product_price_avg_2_prior,
        LAG(last_known_product_price_avg_3_prior) OVER (ORDER BY next_product_cost, id) AS prev_last_known_product_price_avg_3_prior,
        LEAD(last_known_product_price_avg_3_prior) OVER (ORDER BY next_product_cost, id) AS next_last_known_product_price_avg_3_prior,
        LAG(product_cost) OVER (ORDER BY next_product_cost, id) AS prev_product_cost_2,
        LEAD(product_cost) OVER (ORDER BY next_product_cost, id) AS next_product_cost_2,
        ROW_NUMBER() OVER (ORDER BY next_product_cost, id) AS event_seq,
        LAST_VALUE(product_price_avg_3_prior_2) IGNORE NULLS OVER (ORDER BY next_product_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_product_price_avg_3_prior_2_2
    FROM with_windows
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name_lower,
        product_cost,
        next_product_cost,
        last_known_product_price_avg_3_prior,
        last_known_product_price_avg_3_prior_2,
        prev_last_known_product_price_avg_3_prior,
        next_last_known_product_price_avg_3_prior,
        prev_product_cost_2,
        next_product_cost_2,
        event_seq,
        last_known_product_price_avg_3_prior_2_2,
        cumulative_product_price_avg_2_prior - next_product_cost_2 AS cumulative_product_price_avg_2_prior_minus_next_product_cost_2,
        ROUND(last_known_product_price_avg_3_prior_2 / cumulative_product_price_avg_2_prior, 2) AS last_known_product_price_avg_3_prior_2_per_cumulative_product_price_avg_2_prior
    FROM with_windows_suppliers
    WHERE product_cost > 320
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name_lower,
        next_product_cost,
        last_known_product_price_avg_3_prior,
        last_known_product_price_avg_3_prior_2,
        next_last_known_product_price_avg_3_prior,
        next_product_cost_2,
        last_known_product_price_avg_3_prior_2_2,
        RANK() OVER (ORDER BY cumulative_product_price_avg_2_prior_minus_next_product_cost_2 DESC NULLS LAST) AS cumulative_product_price_avg_2_prior_minus_next_product_cost_2_rank,
        ROW_NUMBER() OVER (ORDER BY cumulative_product_price_avg_2_prior_minus_next_product_cost_2 DESC, id) AS event_seq_2
    FROM cleaned
    WHERE event_seq < 8
)
-- Grouped metrics
SELECT
    CASE WHEN next_product_cost_2 >= 750 THEN 'LARGE' ELSE 'SMALL' END AS next_product_cost_2_tier,
    PERCENTILE_APPROX(last_known_product_price_avg_3_prior, 0.5) AS last_known_product_price_avg_3_prior_p50,
    COUNT(*) AS cnt
FROM metrics
GROUP BY CUBE (CASE WHEN next_product_cost_2 >= 750 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY cnt NULLS LAST
