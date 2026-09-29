WITH matched_keys AS (
    -- Set difference on keys
    SELECT plan, status
    FROM subscriptions
    EXCEPT ALL
    SELECT plan, status
    FROM subscriptions
    WHERE plan = 'basic'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM subscriptions s
    JOIN matched_keys k ON s.plan = k.plan AND s.status = k.status
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.city,
        d.name
    FROM kept_rows e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.country,
        p.email
    FROM enriched src
    LEFT JOIN customers p
        ON src.customer_id = p.id
)
SELECT
    customer_id,
    plan,
    end_date,
    monthly_fee,
    status,
    city,
    country,
    email
FROM joined
ORDER BY plan DESC NULLS LAST
