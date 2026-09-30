WITH exploded AS (
    SELECT
        id,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) x AS val
),
remaining_keys AS (
    SELECT currency, id
    FROM exploded
    EXCEPT DISTINCT
    SELECT currency, id
    FROM price_tiers
    WHERE plan = 'basic'
),
reconciled AS (
    SELECT s.*
    FROM exploded s
    INNER JOIN remaining_keys k ON s.currency = k.currency AND s.id = k.id
),
cleaned AS (
    SELECT *, ROUND(val, 1) AS val_rounded
    FROM reconciled
    WHERE currency = 'EUR'
)
SELECT
    id,
    currency,
    val_rounded,
    CASE WHEN currency RLIKE '^(EUR|THB)$' THEN 'GROUP_A' WHEN currency RLIKE '^(USD)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS currency_zone
FROM cleaned
ORDER BY id DESC NULLS FIRST, val_rounded
