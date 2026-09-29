WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC NULLS FIRST, id ASC) AS latest_rank
        FROM customers
    ) ranked
    WHERE latest_rank = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.order_date < s.signup_date
        )
),
deduped_customers AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        name,
        email,
        signup_date,
        is_active,
        latest_rank
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC NULLS FIRST, id ASC) AS rank_in_group
        FROM eligible
    ) ranked
    WHERE rank_in_group = 1
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.is_active,
        latest.discount
    FROM deduped_customers s
    LEFT JOIN LATERAL (
            SELECT c.discount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Grouped metrics
SELECT
    CASE WHEN discount >= 0.32 THEN 'LARGE' ELSE 'SMALL' END AS discount_tier,
    PERCENTILE_APPROX(discount, ARRAY(0.25, 0.5, 0.75)) AS discount_quartiles,
    SUM(discount) AS total_discount,
    APPROX_PERCENTILE(discount, ARRAY(0.25, 0.5, 0.75)) AS discount_quartiles_2,
    PERCENTILE_APPROX(discount, 0.95) AS discount_p95,
    AVG(discount) AS avg_discount
FROM with_last
GROUP BY CASE WHEN discount >= 0.32 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY discount_p95 ASC NULLS FIRST;
