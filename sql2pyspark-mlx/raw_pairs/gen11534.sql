WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS element
    FROM carts
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.element,
        c.segment,
        c.email,
        c.signup_date
    FROM lines src
    LEFT JOIN customers c
        ON src.customer_id <=> c.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.credit_limit,
        p.city,
        p.segment AS customer_segment
    FROM enriched s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
enriched_carts AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date), segment ORDER BY credit_limit NULLS LAST) credit_limit_rank,
        MIN(credit_limit) OVER (PARTITION BY DATE_TRUNC('WEEK', signup_date), customer_segment) signup_date_period_min_credit_limit,
        RANK() OVER (PARTITION BY DATE_TRUNC('month', signup_date) ORDER BY credit_limit DESC NULLS FIRST) credit_limit_rank_2,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment), 4) credit_limit_share
    FROM joined
)
SELECT
    customer_id,
    element,
    segment,
    email,
    signup_date,
    credit_limit,
    city,
    customer_segment,
    credit_limit_rank,
    CASE WHEN city RLIKE '^(Singapore|Chiang Mai)$' THEN 'PRIORITY' WHEN city RLIKE '^(Bangkok|New York)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS city_zone
FROM enriched_carts;
