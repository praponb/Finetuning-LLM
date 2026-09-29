WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS attr_key, attr_value
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY created_at DESC, id, attr_key) AS seq_no,
        LAST_VALUE(attr_key) IGNORE NULLS OVER (PARTITION BY attr_key ORDER BY created_at, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_attr_key
    FROM exploded
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key, customer_id ORDER BY created_at, id, attr_key) AS latest_rank
        FROM scored
    ) ranked
    WHERE latest_rank = 1
)
SELECT
    id,
    customer_id,
    coupon_codes,
    attr_key,
    last_known_attr_key,
    latest_rank
FROM ranked
