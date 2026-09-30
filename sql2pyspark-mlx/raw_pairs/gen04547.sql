WITH filtered AS (
    SELECT
        *,
        TRIM(country) AS clean_country,
        TO_DATE(started_at) AS started_at_date,
        COALESCE(browser, 'DIRECT') AS resolved_browser
    FROM sessions
    WHERE country IS DISTINCT FROM 'SG'
),
matched_keys AS (
    SELECT customer_id
    FROM filtered
    EXCEPT ALL
    SELECT customer_id
    FROM sessions
    WHERE device IN ('mobile', 'tablet', 'desktop')
),
kept_rows AS (
    SELECT s.*
    FROM filtered s
    JOIN matched_keys k ON s.customer_id = k.customer_id
),
joined AS (
    SELECT
        a.customer_id,
        a.device,
        a.browser,
        a.country,
        a.clean_country,
        a.resolved_browser,
        p.signup_date,
        p.credit_limit,
        p.segment
    FROM kept_rows a
    LEFT JOIN customers p
        ON a.customer_id = p.id
    WHERE LENGTH(p.email) >= 8
),
scored AS (
    SELECT
        *,
        COUNT(credit_limit) OVER (PARTITION BY browser) AS browser_count_credit_limit,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY credit_limit DESC NULLS LAST) AS credit_limit_dense_rank
    FROM joined
),
scored_sessions AS (
    SELECT
        device,
        browser,
        country,
        signup_date,
        credit_limit,
        browser_count_credit_limit,
        credit_limit_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY device, browser ORDER BY credit_limit DESC) AS credit_limit_percent_rank,
        AVG(browser_count_credit_limit) OVER (PARTITION BY browser) AS browser_avg_browser_count_credit_limit,
        STDDEV_SAMP(browser_count_credit_limit) OVER (PARTITION BY browser, country) AS browser_stddev_samp_browser_count_credit_limit
    FROM scored
),
scored_sessions_2 AS (
    SELECT
        browser,
        browser_count_credit_limit,
        credit_limit_dense_rank,
        browser_avg_browser_count_credit_limit,
        DENSE_RANK() OVER (PARTITION BY device ORDER BY credit_limit_percent_rank DESC NULLS LAST) AS credit_limit_percent_rank_dense_rank,
        PERCENT_RANK() OVER (ORDER BY browser_count_credit_limit DESC) AS browser_count_credit_limit_percent_rank,
        DENSE_RANK() OVER (PARTITION BY browser ORDER BY browser_count_credit_limit DESC NULLS LAST) AS browser_count_credit_limit_dense_rank
    FROM scored_sessions
),
filtered_sessions AS (
    SELECT
        browser,
        credit_limit_dense_rank,
        browser_avg_browser_count_credit_limit,
        credit_limit_percent_rank_dense_rank,
        browser_count_credit_limit_percent_rank,
        browser_count_credit_limit_dense_rank,
        LOWER(browser) AS browser_lower,
        ROUND(browser_avg_browser_count_credit_limit / browser_count_credit_limit, 2) AS browser_avg_browser_count_credit_limit_per_browser_count_credit_limit,
        COALESCE(browser, 'N/A') AS browser_filled
    FROM scored_sessions_2
    WHERE browser_count_credit_limit = 8
),
totals AS (
    SELECT
        CASE WHEN browser_avg_browser_count_credit_limit >= 17 THEN 'PREMIUM' ELSE 'STANDARD' END AS browser_avg_browser_count_credit_limit_tier,
        browser,
        SUM(credit_limit_percent_rank_dense_rank) AS total_credit_limit_percent_rank_dense_rank,
        ROUND(SUM(browser_count_credit_limit_percent_rank) / NULLIF(COUNT(CASE WHEN browser = 'Firefox' THEN 1 END), 0), 2) AS browser_count_credit_limit_percent_rank_per_firefox,
        SUM(credit_limit_percent_rank_dense_rank) AS total_credit_limit_percent_rank_dense_rank_2,
        ROUND(AVG(credit_limit_dense_rank), 2) AS avg_credit_limit_dense_rank,
        MIN(browser_count_credit_limit_percent_rank) AS min_browser_count_credit_limit_percent_rank,
        SORT_ARRAY(COLLECT_SET(browser)) AS browser_list
    FROM filtered_sessions
    GROUP BY GROUPING SETS ((CASE WHEN browser_avg_browser_count_credit_limit >= 17 THEN 'PREMIUM' ELSE 'STANDARD' END, browser), (CASE WHEN browser_avg_browser_count_credit_limit >= 17 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
)
SELECT
    browser,
    total_credit_limit_percent_rank_dense_rank,
    total_credit_limit_percent_rank_dense_rank_2,
    avg_credit_limit_dense_rank,
    min_browser_count_credit_limit_percent_rank
FROM totals
ORDER BY avg_credit_limit_dense_rank DESC;
