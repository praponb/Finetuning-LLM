WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(discount_pct, 2) AS discount_pct_rounded,
        DATEDIFF(ends_on, starts_on) AS days_starts_on_to_ends_on
    FROM promotions
    WHERE channel IN ('web', 'store', 'partner')
)
SELECT id, channel
FROM filtered
INTERSECT ALL
SELECT id, channel
FROM promotions
WHERE starts_on BETWEEN '2023-03-01' AND '2025-09-01';
