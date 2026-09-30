WITH base AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        CAST(started_at AS DATE) AS started_at_day,
        CASE WHEN device = 'mobile' THEN 1 ELSE 0 END AS is_mobile,
        COALESCE(country, 'N/A') AS country_clean
    FROM sessions
    WHERE customer_id IS NOT NULL
),
joined AS (
    SELECT e.*, d.credit_limit
    FROM base e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
enriched AS (
    SELECT
        id,
        country,
        is_mobile,
        country_clean,
        AVG(is_mobile) OVER (PARTITION BY device ORDER BY is_mobile, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS is_mobile_avg_3,
        COUNT(credit_limit) OVER (PARTITION BY country) AS country_count_credit_limit
    FROM joined
),
cleaned AS (
    SELECT
        id,
        is_mobile,
        country_clean,
        is_mobile_avg_3,
        TRIM(country) AS clean_country,
        COALESCE(country, 'OTHER') AS resolved_country
    FROM enriched
    WHERE is_mobile_avg_3 BETWEEN 24 AND 64
)
SELECT
    CASE WHEN is_mobile >= 71 THEN 'LARGE' ELSE 'SMALL' END AS is_mobile_tier,
    PERCENTILE_APPROX(is_mobile, ARRAY(0.25, 0.5, 0.75)) AS is_mobile_quartiles,
    APPROX_PERCENTILE(is_mobile, ARRAY(0.25, 0.5, 0.75)) AS is_mobile_quartiles_2
FROM cleaned
GROUP BY CASE WHEN is_mobile >= 71 THEN 'LARGE' ELSE 'SMALL' END;
