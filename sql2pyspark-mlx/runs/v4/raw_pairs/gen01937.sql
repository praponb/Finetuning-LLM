WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) x AS element
)
SELECT
    *,
    AVG(element) OVER w_part plan_avg_element,
    DENSE_RANK() OVER (ORDER BY element DESC) element_dense_rank
FROM flattened
WINDOW w_part AS (PARTITION BY plan)
