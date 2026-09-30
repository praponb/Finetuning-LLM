WITH exploded AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS k, v
),
filtered AS (
    SELECT
        k,
        v,
        COALESCE(k, 'unassigned') AS k_filled
    FROM exploded
    WHERE k IS NOT NULL
      AND SIZE(coupon_codes) > 1
),
base AS (
    SELECT
        *,
        INITCAP(TRIM(k)) AS k_title,
        COALESCE(k, 'OTHER') AS resolved_k
    FROM filtered
    WHERE k IN ('device', 'weight_kg')
),
base_carts AS (
    SELECT
        k,
        v,
        k_filled,
        k_title,
        resolved_k,
        LOWER(k) AS k_lower
    FROM base
    WHERE k IS NOT DISTINCT FROM 'gift'
),
base_carts_2 AS (
    SELECT
        *,
        CASE WHEN k IN ('utm_source', 'gift') THEN 'Tier 1' ELSE 'Other' END AS k_group,
        TRIM(k) AS clean_k
    FROM base_carts
    WHERE k IS NOT NULL
),
cleaned AS (
    SELECT
        *,
        TRIM(k) AS clean_k_2,
        LOWER(k) AS k_lower_2
    FROM base_carts_2
    WHERE k IS NOT NULL
      AND k IN ('weight_kg', 'device')
),
daily AS (
    SELECT
        k,
        COUNT_IF(k = 'utm_source') AS utm_source_events,
        SIZE(COLLECT_SET(k)) AS num_ks
    FROM cleaned
    GROUP BY k
)
SELECT
    k,
    utm_source_events,
    num_ks,
    CASE WHEN k RLIKE '^(weight_kg|utm_source)$' THEN 'GROUP_B' WHEN k RLIKE '^(device|gift)$' THEN 'EXTENDED' ELSE 'CORE' END AS k_zone
FROM daily
ORDER BY num_ks DESC;
