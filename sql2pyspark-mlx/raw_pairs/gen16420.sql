WITH filtered AS (
    SELECT
        id,
        country,
        city,
        rating,
        CASE WHEN city = 'New York' THEN 1 ELSE 0 END AS is_new_york
    FROM suppliers
    WHERE city IS NOT NULL
),
enriched AS (
    SELECT
        *,
        LAG(rating) OVER (PARTITION BY country ORDER BY rating, id) AS prev_rating,
        LAG(is_new_york) OVER (PARTITION BY country ORDER BY rating, id) AS prev_is_new_york,
        LEAD(is_new_york) OVER (PARTITION BY country ORDER BY rating, id) AS next_is_new_york,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS event_seq
    FROM filtered
    WHERE city = 'Berlin'
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY is_new_york DESC NULLS FIRST, id) AS rn
        FROM enriched
    ) t
    WHERE rn = 1
),
latest_match AS (
    SELECT
        s.id,
        s.country,
        s.rating,
        s.is_new_york,
        s.prev_rating,
        s.prev_is_new_york,
        s.event_seq,
        s.rn,
        latest.brand,
        latest.product_cost
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.brand, c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    SELECT
        id,
        country,
        prev_is_new_york,
        event_seq,
        STDDEV_SAMP(product_cost) OVER (PARTITION BY brand) AS brand_stddev_samp_product_cost,
        SUM(is_new_york) OVER (PARTITION BY brand ORDER BY product_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_new_york,
        SUM(is_new_york) OVER (ORDER BY prev_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_is_new_york
    FROM latest_match
),
scored AS (
    SELECT
        *,
        SUM(prev_is_new_york) OVER (PARTITION BY country ORDER BY brand_stddev_samp_product_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_prev_is_new_york,
        MAX(prev_is_new_york) OVER (PARTITION BY country ORDER BY brand_stddev_samp_product_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_prev_is_new_york,
        COUNT(prev_is_new_york) OVER (PARTITION BY country ORDER BY brand_stddev_samp_product_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_prev_is_new_york,
        AVG(prev_is_new_york) OVER (PARTITION BY country ORDER BY brand_stddev_samp_product_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_prev_is_new_york,
        FIRST_VALUE(prev_is_new_york, TRUE) OVER (PARTITION BY country ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_is_new_york
    FROM with_windows
),
eligible AS (
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.price > 1150
        )
),
stats AS (
    SELECT
        CASE WHEN running_count_prev_is_new_york >= 13 THEN 'HIGH' ELSE 'LOW' END AS running_count_prev_is_new_york_tier,
        CASE WHEN event_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
        MAX(prev_is_new_york) FILTER (WHERE country = 'JP') AS jp_max_prev_is_new_york,
        COUNT(DISTINCT country) AS distinct_countries,
        SIZE(COLLECT_SET(country)) AS num_countries,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE country = 'US') AS us_rows
    FROM eligible
    GROUP BY running_count_prev_is_new_york_tier, event_seq_tier
    HAVING COUNT(*) FILTER (WHERE country = 'US') < 1000
)
SELECT
    running_count_prev_is_new_york_tier,
    event_seq_tier,
    jp_max_prev_is_new_york,
    us_rows
FROM stats
