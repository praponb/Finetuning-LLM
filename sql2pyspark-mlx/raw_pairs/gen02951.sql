WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        status,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        LENGTH(plan) AS plan_length
    FROM subscriptions
    WHERE end_date >= DATE '2023-08-02'
      AND customer_id IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.days_end_date_to_start_date,
        s.plan_length,
        d.credit_limit,
        d.country
    FROM filtered s
    INNER JOIN customers d
        ON s.customer_id = d.id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, customer_id
    FROM enriched
    INTERSECT
    SELECT id, customer_id
    FROM subscriptions
    WHERE monthly_fee IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.days_end_date_to_start_date,
        src.credit_limit,
        b.country AS customer_country,
        b.credit_limit AS customer_credit_limit
    FROM reconciled src
    INNER JOIN customers b
        ON src.customer_id <=> b.id
),
enriched_subscriptions_2 AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.segment,
        c.city
    FROM enriched_subscriptions e
    LEFT JOIN customers c
        ON e.customer_id = c.id
    WHERE c.signup_date >= '2023-10-22'
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.days_end_date_to_start_date,
        src.customer_country,
        src.segment,
        src.city,
        d.signup_date,
        d.segment AS customer_segment,
        d.age
    FROM enriched_subscriptions_2 src
    LEFT JOIN customers d
        ON src.customer_id = d.id
)
SELECT
    id,
    customer_id,
    segment,
    city,
    CASE WHEN city RLIKE '^(Berlin|Chiang Mai)$' THEN 'GROUP_A' WHEN city RLIKE '^(Bangkok|New York)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS city_zone
FROM with_dims
ORDER BY city DESC;
