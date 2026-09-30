WITH flattened AS (
    SELECT
        id,
        plan,
        EXPLODE_OUTER(discounts) AS val
    FROM price_tiers
)
SELECT
    id,
    plan,
    val
FROM flattened
ORDER BY val DESC, id
