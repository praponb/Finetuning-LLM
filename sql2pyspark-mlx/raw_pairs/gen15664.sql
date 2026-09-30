WITH cleaned AS (
    SELECT
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        ROUND(age / credit_limit, 2) AS age_per_credit_limit,
        CASE WHEN country IN ('DE', 'SG') THEN 'Priority' ELSE 'Other' END AS country_group,
        COALESCE(city, 'UNKNOWN') AS resolved_city
    FROM customers
    WHERE city IS NOT NULL
)
SELECT
    s.*,
    top1.device,
    top1.session_country
FROM cleaned s
JOIN LATERAL (
        SELECT c.device, c.country AS session_country
        FROM sessions c
        WHERE c.customer_id = s.id
          AND c.started_at <= s.signup_date
        ORDER BY c.started_at DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE;
