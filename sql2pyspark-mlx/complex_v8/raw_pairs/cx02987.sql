WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat
    FROM tickets
    WHERE csat >= 1
),
rollup_base AS (
    -- Group-level totals
    SELECT
        CASE WHEN requester_id >= 22 THEN 'large' WHEN requester_id >= 7 THEN 'medium' ELSE 'small' END AS requester_id_tier,
        MIN(csat) AS min_csat,
        SUM(CASE WHEN channel IN ('phone', 'web', 'chat') THEN requester_id ELSE 0 END) AS requester_id_channel_in_phone_web_chat,
        COUNT(*) FILTER (WHERE channel NOT IN ('phone', 'web')) AS n_channel_not_in_phone_web,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list
    FROM cleaned
    WHERE csat <= 4
    GROUP BY 1
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM rollup_base
    WHERE requester_id_channel_in_phone_web_chat > (SELECT MIN(requester_id_channel_in_phone_web_chat) FROM rollup_base)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(n_channel_not_in_phone_web) IGNORE NULLS OVER (ORDER BY requester_id_channel_in_phone_web_chat, requester_id_tier ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_n_channel_not_in_phone_web,
        RANK() OVER w_min_csat_n_channel_not_in_ph AS n_channel_not_in_phone_web_rank,
        MIN(n_channel_not_in_phone_web) OVER w_n_channel_not_in_phone_web_m AS n_channel_not_in_phone_web_min_csat_min_n_channe,
        NTILE(3) OVER (ORDER BY requester_id_channel_in_phone_web_chat, requester_id_tier) AS requester_id_channel_in_phone_web_chat_tercile,
        ROUND(min_csat - AVG(min_csat) OVER (PARTITION BY n_channel_not_in_phone_web, min_csat), 2) AS min_csat_vs_n_channel_not_in_phone_web_min_csat_
    FROM without_matches
    WINDOW w_min_csat_n_channel_not_in_ph AS (PARTITION BY min_csat, n_channel_not_in_phone_web ORDER BY n_channel_not_in_phone_web ASC NULLS LAST), w_n_channel_not_in_phone_web_m AS (PARTITION BY n_channel_not_in_phone_web, min_csat)
),
per_label AS (
    -- One row per channel_list_item
    SELECT
        s.requester_id_tier,
        s.min_csat,
        s.requester_id_channel_in_phone_web_chat,
        s.n_channel_not_in_phone_web,
        s.first_known_n_channel_not_in_phone_web,
        s.n_channel_not_in_phone_web_rank,
        s.n_channel_not_in_phone_web_min_csat_min_n_channe,
        s.requester_id_channel_in_phone_web_chat_tercile,
        s.min_csat_vs_n_channel_not_in_phone_web_min_csat_,
        t.channel_list_item_pos,
        t.channel_list_item
    FROM sequenced s
    LATERAL VIEW OUTER POSEXPLODE(s.channel_list) t AS channel_list_item_pos, channel_list_item
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_label
    WHERE n_channel_not_in_phone_web_rank > (SELECT AVG(n_channel_not_in_phone_web_rank) FROM per_label)
)
-- Final projection
SELECT
    requester_id_tier,
    min_csat,
    n_channel_not_in_phone_web,
    first_known_n_channel_not_in_phone_web,
    n_channel_not_in_phone_web_min_csat_min_n_channe,
    requester_id_channel_in_phone_web_chat_tercile,
    min_csat_vs_n_channel_not_in_phone_web_min_csat_,
    channel_list_item_pos,
    channel_list_item,
    LOWER(REGEXP_REPLACE(TRIM(channel_list_item), ' ', '_')) AS channel_list_item_slug
FROM qualified
ORDER BY first_known_n_channel_not_in_phone_web NULLS LAST
