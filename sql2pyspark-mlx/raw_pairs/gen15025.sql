WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(browser, '/', country) AS browser_country,
        COALESCE(browser, 'UNKNOWN') AS browser_clean
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-04-10 16:00:00'
      AND browser IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(country) IGNORE NULLS OVER w_ordered AS first_known_country,
        ROW_NUMBER() OVER (ORDER BY started_at DESC, id) AS event_seq
    FROM cleaned
    WHERE customer_id IS NULL
    WINDOW w_ordered AS (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.device,
        src.browser,
        src.country,
        src.browser_country,
        src.first_known_country,
        c.city,
        c.age,
        c.country AS customer_country
    FROM metrics src
    LEFT JOIN customers c
        ON src.customer_id <=> c.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(city) AS city_upper,
        COALESCE(country, 'DIRECT') AS country_filled
    FROM joined
    WHERE browser IS NOT NULL
      AND customer_country IS DISTINCT FROM 'TH'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(age) OVER w_running AS last7_min_age,
        COUNT(age) OVER w_running AS last7_count_age,
        SUM(age) OVER w_running AS last7_sum_age,
        AVG(age) OVER w_running AS last7_avg_age
    FROM filtered
    WINDOW w_running AS (PARTITION BY customer_country ORDER BY age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    customer_id,
    browser,
    country,
    browser_country,
    first_known_country,
    city,
    age,
    country_filled
FROM scored
ORDER BY country NULLS FIRST, browser;
