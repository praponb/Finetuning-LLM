WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        browser,
        CASE WHEN country = 'UK' THEN 1 ELSE 0 END AS is_uk
    FROM sessions
    WHERE browser IN ('Edge', 'Firefox')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(is_uk) OVER w_hist AS device_max_is_uk,
        LAST_VALUE(is_uk) IGNORE NULLS OVER w_grp AS last_known_is_uk,
        SUM(is_uk) OVER w_grp AS cumulative_is_uk
    FROM valid
    WINDOW w_hist AS (PARTITION BY device), w_grp AS (PARTITION BY customer_id ORDER BY is_uk, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.session_id IN (11, 15, 17, 35)
        )
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.device,
        e.is_uk,
        c.email
    FROM eligible e
    LEFT JOIN customers c
        ON e.customer_id <=> c.id
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.web_event_event_type
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched_sessions AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(is_uk) OVER (PARTITION BY device ORDER BY is_uk, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS is_uk_avg_2,
        LAG(is_uk, 1) OVER (ORDER BY is_uk, id) AS prev_is_uk,
        RANK() OVER (PARTITION BY web_event_event_type ORDER BY is_uk DESC NULLS FIRST) AS is_uk_rank
    FROM latest_match
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY prev_is_uk DESC NULLS LAST, id) AS row_num
    FROM enriched_sessions
    QUALIFY row_num = 1
)
-- Join a dimension and aggregate
SELECT
    d.country,
    a.device,
    COUNT(*) AS cnt,
    MAX(a.is_uk) AS max_is_uk,
    GROUPING_ID() AS gid
FROM ranked a
LEFT JOIN customers d ON a.customer_id = d.id
GROUP BY ROLLUP (d.country, a.device)
ORDER BY cnt NULLS LAST;
