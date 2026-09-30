WITH with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.name,
        d.age
    FROM sessions a
    INNER JOIN customers d
        ON a.customer_id = d.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(age) OVER w_recent AS next_age,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM with_dims
    WINDOW w_recent AS (PARTITION BY browser ORDER BY age, id)
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.device,
        s.browser,
        s.country,
        s.name,
        s.age,
        s.next_age,
        x.page,
        x.duration_sec
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.page, c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.email,
        p.country AS customer_country
    FROM with_latest a
    INNER JOIN customers p
        ON a.customer_id = p.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(age / SUM(age) OVER (PARTITION BY device), 4) AS age_share,
        DENSE_RANK() OVER w_ordered AS age_dense_rank
    FROM joined
    WINDOW w_ordered AS (PARTITION BY customer_country ORDER BY age DESC NULLS LAST)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.duration_sec >= 286
        )
),
enriched_sessions AS (
    -- Ranking and gap features
    SELECT
        id,
        device,
        browser,
        country,
        name,
        age_share,
        MIN(age_dense_rank) OVER (PARTITION BY page) AS page_min_age_dense_rank,
        CUME_DIST() OVER (ORDER BY age NULLS FIRST) AS age_cume_dist
    FROM kept
)
-- Aggregate with conditional metrics
SELECT
    browser,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
    APPROX_PERCENTILE(age_cume_dist, 0.5) AS age_cume_dist_p50,
    COUNT(CASE WHEN device = 'tablet' THEN 1 END) AS tablet_count,
    SIZE(COLLECT_SET(device)) AS num_devices,
    SIZE(COLLECT_SET(browser)) AS num_browsers
FROM enriched_sessions
GROUP BY browser
ORDER BY tablet_count NULLS FIRST
