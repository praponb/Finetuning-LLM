WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(browser, 1, 4) AS browser_prefix,
        COALESCE(country, 'UNKNOWN') AS country_clean
    FROM sessions
    WHERE country IS NOT NULL
      AND started_at < '2024-06-01'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.email,
        c.signup_date
    FROM sessions_sessions e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        browser_prefix,
        country_clean,
        is_new_burst,
        session_id,
        email,
        signup_date,
        MAX(session_id) OVER (PARTITION BY country) AS country_max_session_id,
        LEAD(session_id, 1) OVER (PARTITION BY browser ORDER BY is_new_burst, id) AS next_session_id,
        AVG(is_new_burst) OVER (ORDER BY session_id, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS is_new_burst_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY device, country ORDER BY session_id DESC, id) AS seq_no
    FROM joined
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_windows src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.signup_date BETWEEN sub.start_date AND sub.end_date
)
-- Window metrics over several different partitions
SELECT
    *,
    STDDEV_SAMP(country_max_session_id) OVER (PARTITION BY browser) AS browser_stddev_samp_country_max_session_id,
    LAST_VALUE(seq_no, TRUE) OVER (PARTITION BY country ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_seq_no,
    MIN(is_new_burst_avg_5_prior) OVER (PARTITION BY plan) AS plan_min_is_new_burst_avg_5_prior,
    COUNT(country_max_session_id) OVER (PARTITION BY browser ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS country_max_session_id_count_30d,
    NTILE(4) OVER (PARTITION BY customer_id ORDER BY session_id DESC, id, subscription_id) AS session_id_bucket
FROM matched;
