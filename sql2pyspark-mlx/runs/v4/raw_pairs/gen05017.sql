WITH flattened AS (
    SELECT
        id,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, value
),
filtered AS (
    SELECT
        *,
        LOWER(k) AS k_lower,
        INITCAP(TRIM(k)) AS k_title,
        COALESCE(k, 'DIRECT') AS k_filled
    FROM flattened
    WHERE k = 'weight_kg'
),
remaining_keys AS (
    SELECT id
    FROM filtered
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
),
reconciled AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.id = k.id
),
base AS (
    SELECT
        k,
        k_lower,
        k_title,
        k_filled,
        SUBSTRING(k, 1, 4) AS k_prefix
    FROM reconciled
    WHERE SIZE(coupon_codes) >= 1
),
cleaned AS (
    SELECT
        k,
        k_lower,
        k_title,
        k_filled,
        k_prefix,
        INITCAP(TRIM(k)) AS k_title_2,
        COALESCE(k, 'UNKNOWN') AS resolved_k
    FROM base
    WHERE k IS NOT NULL
      AND k IS NOT NULL
),
valid AS (
    SELECT *, CASE WHEN k = 'weight_kg' THEN 1 ELSE 0 END AS is_weight_kg
    FROM cleaned
    WHERE k IS NOT NULL
)
SELECT
    k,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list,
    COUNT(CASE WHEN k = 'gift' THEN 1 END) AS gift_count,
    SIZE(COLLECT_SET(k)) AS num_ks
FROM valid
GROUP BY k
HAVING SIZE(COLLECT_SET(k)) > 1
ORDER BY num_ks DESC;
