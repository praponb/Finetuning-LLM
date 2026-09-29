WITH exploded AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, value
),
valid AS (
    SELECT
        *,
        INITCAP(TRIM(k)) AS k_title,
        ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS is_spring10
    FROM exploded
    WHERE k IS NULL
),
filtered AS (
    SELECT
        id,
        customer_id,
        k,
        value,
        k_title,
        is_spring10,
        CAST(is_spring10 AS INT) AS is_spring10_flag,
        COALESCE(k, 'DIRECT') AS k_filled
    FROM valid
    WHERE NOT is_spring10
),
scored AS (
    SELECT
        *,
        NTILE(3) OVER w_part AS is_spring10_flag_bucket,
        NTILE(4) OVER w_part_2 AS is_spring10_flag_bucket_2,
        NTILE(3) OVER w_part AS is_spring10_flag_bucket_3,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY is_spring10_flag DESC, id, k) AS event_seq
    FROM filtered
    WINDOW w_part AS (PARTITION BY k ORDER BY is_spring10_flag, id, k), w_part_2 AS (PARTITION BY customer_id ORDER BY is_spring10_flag, id, k)
),
enriched AS (
    SELECT
        s.*,
        d.age,
        d.name
    FROM scored s
    LEFT JOIN customers d
        ON s.customer_id = d.id
)
SELECT
    id,
    k,
    value,
    k_title,
    is_spring10,
    is_spring10_flag,
    is_spring10_flag_bucket_2,
    event_seq,
    age,
    CASE WHEN k RLIKE '^(weight_kg|device)$' THEN 'OTHER_ZONE' WHEN k RLIKE '^(utm_source|gift)$' THEN 'CORE' ELSE 'PRIORITY' END AS k_zone
FROM enriched
