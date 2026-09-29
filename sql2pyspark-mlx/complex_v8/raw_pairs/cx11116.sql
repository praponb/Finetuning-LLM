WITH filtered_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        csat,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        COALESCE(requester_id, 0) AS requester_id_filled
    FROM tickets
    WHERE requester_id < 21
      AND priority IN ('P2', 'P4', 'P1')
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        opened_at,
        channel,
        csat,
        requester_id_filled,
        LAG(csat, 1, 0) OVER (PARTITION BY channel, csat ORDER BY opened_at ASC, id) AS prev_csat,
        SUM(requester_id) OVER (PARTITION BY agent_id, channel ORDER BY opened_at_month NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS requester_id_sum_3,
        FIRST_VALUE(opened_at_month) OVER (PARTITION BY channel, csat ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_at_month,
        NTH_VALUE(csat, 2) OVER (PARTITION BY channel, csat ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY YEAR(opened_at_month)), 2) AS requester_id_vs_opened_at_month_year_avg,
        SIZE(COLLECT_SET(channel) OVER w_opened_at_month_year) AS distinct_channel_in_opened_at_month_year,
        ROUND(CUME_DIST() OVER (PARTITION BY channel, csat ORDER BY opened_at ASC), 3) AS opened_at_cume_dist,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY agent_id, channel ORDER BY opened_at_month NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id,
        LAG(requester_id_filled, 2) OVER (PARTITION BY channel, csat ORDER BY opened_at ASC, id) AS prev2_requester_id_filled,
        ROUND(requester_id_filled - AVG(requester_id_filled) OVER (PARTITION BY YEAR(opened_at_month)), 2) AS requester_id_filled_vs_opened_at_month_year_avg
    FROM filtered_tickets
    WINDOW w_opened_at_month_year AS (PARTITION BY YEAR(opened_at_month))
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(opened_at_cume_dist, requester_id_filled) AS greatest_opened_at_cume_dist_requester_id_filled,
        ROUND(opened_at_cume_dist * 1.1, 2) AS opened_at_cume_dist_adj
    FROM metrics
    WHERE channel = 'chat'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(csat) OVER (PARTITION BY MONTH(opened_at), second_csat) AS opened_at_month_second_csat_min_csat,
        SUM(opened_at_cume_dist_adj) OVER (PARTITION BY MONTH(opened_at), second_csat) AS opened_at_month_second_csat_sum_opened_at_cume_d,
        ROUND(first_known_requester_id - AVG(first_known_requester_id) OVER (PARTITION BY MONTH(opened_at), second_csat), 2) AS first_known_requester_id_vs_opened_at_month_seco,
        RANK() OVER (PARTITION BY csat, second_csat ORDER BY requester_id DESC) AS requester_id_rank,
        RANK() OVER w_csat_first_opened_at_month_m AS requester_id_vs_opened_at_month_year_avg_rank,
        COUNT(*) OVER (PARTITION BY MONTH(opened_at), second_csat) AS rows_in_opened_at_month_second_csat,
        MAX(csat) OVER (PARTITION BY csat, second_csat ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_csat_so_far,
        SUM(opened_at_cume_dist) OVER (PARTITION BY MONTH(opened_at), second_csat) AS opened_at_month_second_csat_sum_opened_at_cume_d_2
    FROM enriched
    WINDOW w_csat_first_opened_at_month_m AS (PARTITION BY csat, DATE_TRUNC('MONTH', first_opened_at_month) ORDER BY requester_id_vs_opened_at_month_year_avg ASC NULLS LAST, id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_channel_in_opened_at_month_year,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        SUM(prev2_requester_id_filled) FILTER (WHERE channel IN ('chat', 'web')) AS prev2_requester_id_filled_channel_in_chat_web,
        ROUND(100 * COUNT(CASE WHEN channel <> 'phone' THEN 1 END) / COUNT(*), 1) AS pct_channel_phone,
        MAX(opened_at) AS last_opened_at,
        SUM(CASE WHEN channel NOT IN ('web', 'email') THEN prev_csat ELSE 0 END) AS prev_csat_channel_not_in_web_email
    FROM sequenced
    WHERE distinct_channel_in_opened_at_month_year > 5
    GROUP BY 1
    HAVING AVG(requester_id_sum_3) > 23
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_all_by_last_opened_at_ties AS last_opened_at_rank,
        COUNT(*) OVER w_distinct_channel_in_opened_a AS rows_in_distinct_channel_in_opened_at_month_year
    FROM per_group
    WINDOW w_all_by_last_opened_at_ties AS (ORDER BY last_opened_at), w_distinct_channel_in_opened_a AS (PARTITION BY distinct_channel_in_opened_at_month_year)
)
-- Final projection
SELECT
    distinct_channel_in_opened_at_month_year,
    channel_list,
    last_opened_at,
    prev_csat_channel_not_in_web_email,
    last_opened_at_rank,
    rows_in_distinct_channel_in_opened_at_month_year,
    FLOOR(last_opened_at_rank / 2) * 2 AS last_opened_at_rank_bucket
FROM with_windows
ORDER BY distinct_channel_in_opened_at_month_year, last_opened_at_rank_bucket NULLS LAST
