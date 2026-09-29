WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(email) clean_email,
        COALESCE(city, 'N/A') resolved_city
    FROM customers
    WHERE signup_date >= DATE '2023-09-03'
      AND city = 'Singapore'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(age, 1) OVER (PARTITION BY DATE_TRUNC('week', signup_date), segment ORDER BY signup_date, id) next_age,
        LAST_VALUE(credit_limit) IGNORE NULLS OVER (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_credit_limit,
        AVG(credit_limit) OVER (ORDER BY age, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) credit_limit_avg_2
    FROM filtered
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        resolved_city,
        last_known_credit_limit,
        credit_limit_avg_2,
        LEAD(credit_limit_avg_2, 1) OVER (PARTITION BY country ORDER BY credit_limit, id) next_credit_limit_avg_2,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date DESC, id) event_seq,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY last_known_credit_limit, id) event_seq_2
    FROM enriched
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY event_seq_2 DESC, id) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    country,
    city,
    signup_date,
    is_active,
    credit_limit,
    resolved_city,
    next_credit_limit_avg_2,
    event_seq,
    CASE WHEN city RLIKE '^(New York|London)$' THEN 'EXTENDED' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS city_zone
FROM deduped
ORDER BY event_seq NULLS FIRST, country DESC
