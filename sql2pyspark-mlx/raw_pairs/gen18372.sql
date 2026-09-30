WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM sessions s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.page IN ('/home', '/checkout')
        )
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.web_event_page
    FROM eligible s
    JOIN LATERAL (
            SELECT c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(web_event_page, TRUE) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_web_event_page,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country
    FROM latest_match
    WHERE customer_id = 9
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM enriched src
    INNER JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.started_at >= sub.start_date
       AND src.started_at <= DATE_ADD(sub.end_date, 7)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        browser,
        web_event_page,
        first_known_country,
        subscription_id,
        plan,
        monthly_fee,
        NTILE(10) OVER (PARTITION BY web_event_page, country ORDER BY subscription_id, id, subscription_id) AS subscription_id_bucket,
        ROW_NUMBER() OVER (ORDER BY subscription_id, id, subscription_id) AS seq_no
    FROM attributed
)
-- Multi-level aggregation
SELECT
    CASE WHEN subscription_id_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS subscription_id_bucket_tier,
    plan,
    COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
    COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count,
    SUM(CASE WHEN web_event_page = '/product' THEN monthly_fee ELSE 0 END) AS product_monthly_fee,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list_2,
    GROUPING_ID() AS grouping_level
FROM scored
GROUP BY ROLLUP (CASE WHEN subscription_id_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END, plan)
