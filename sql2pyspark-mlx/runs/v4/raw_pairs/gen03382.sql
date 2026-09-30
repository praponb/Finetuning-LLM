WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY paid_at, id) AS dedup_rank
    FROM payments
    QUALIFY dedup_rank = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM ranked
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(is_new_session) OVER w_part AS is_new_session_count_7d,
        MAX(amount) OVER w_recent AS order_id_max_amount
    FROM sessionized
    WINDOW w_part AS (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY order_id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_session_tier,
        island_id,
        ROUND(AVG(is_new_session), 2) AS avg_is_new_session,
        SORT_ARRAY(COLLECT_SET(method)) AS method_list,
        SUM(is_new_session) AS total_is_new_session
    FROM scored
    GROUP BY CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END, island_id
)
SELECT
    is_new_session_tier,
    island_id,
    avg_is_new_session,
    method_list
FROM stats
ORDER BY avg_is_new_session NULLS FIRST;
