WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        tags,
        SUBSTRING(TRIM(priority), 1, 3) AS priority_prefix,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id,
        opened_at + INTERVAL 1 DAYS AS opened_at_plus_1d
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'outage')
      AND priority = 'P1'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        opened_at,
        tags,
        priority_prefix,
        opened_at_plus_1d,
        RANK() OVER (PARTITION BY TO_DATE(opened_at) ORDER BY opened_at_plus_1d) AS opened_at_plus_1d_rank,
        ROUND(AVG(csat_per_requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at_plus_1d)), 2) AS opened_at_plus_1d_month_avg_csat_per_requester_i,
        MAX(csat_per_requester_id) OVER (PARTITION BY channel, MONTH(opened_at) ORDER BY csat_per_requester_id DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS csat_per_requester_id_max_4,
        RANK() OVER w_channel_opened_at_month_by_c_2 AS csat_per_requester_id_rank
    FROM cleaned
    WINDOW w_channel_opened_at_month_by_c_2 AS (PARTITION BY channel, MONTH(opened_at) ORDER BY csat_per_requester_id DESC, id)
),
per_label AS (
    -- One row per tag
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority_prefix,
        s.opened_at_plus_1d,
        s.opened_at_plus_1d_rank,
        s.opened_at_plus_1d_month_avg_csat_per_requester_i,
        s.csat_per_requester_id_max_4,
        s.csat_per_requester_id_rank,
        t.tag
    FROM with_trends s
    LATERAL VIEW EXPLODE(s.tags) t AS tag
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(csat_per_requester_id_max_4) OVER w_opened_at_to_date_agent_id AS opened_at_to_date_agent_id_sum_csat_per_requeste,
        ROUND(opened_at_plus_1d_rank / NULLIF(SUM(opened_at_plus_1d_rank) OVER (PARTITION BY TO_DATE(opened_at), agent_id), 0), 4) AS opened_at_plus_1d_rank_share_of_opened_at_to_dat,
        ROUND(AVG(opened_at_plus_1d_rank) OVER (ORDER BY csat_per_requester_id_rank ASC, id, tag ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS opened_at_plus_1d_rank_avg_2_prior,
        RANK() OVER (ORDER BY csat_per_requester_id_rank ASC, id, tag) AS csat_per_requester_id_rank_rank
    FROM per_label
    WINDOW w_opened_at_to_date_agent_id AS (PARTITION BY TO_DATE(opened_at), agent_id)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        opened_at,
        opened_at_plus_1d,
        opened_at_plus_1d_rank,
        opened_at_plus_1d_month_avg_csat_per_requester_i,
        csat_per_requester_id_max_4,
        csat_per_requester_id_rank,
        tag,
        opened_at_to_date_agent_id_sum_csat_per_requeste,
        opened_at_plus_1d_rank_share_of_opened_at_to_dat,
        opened_at_plus_1d_rank_avg_2_prior,
        csat_per_requester_id_rank_rank,
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_label
    FROM windowed
),
per_group AS (
    -- Group-level totals
    SELECT
        tag,
        MAX(opened_at) AS last_opened_at,
        ROUND(STDDEV(csat_per_requester_id_rank_rank), 2) AS stddev_csat_per_requester_id_rank_rank
    FROM scored
    GROUP BY tag
)
SELECT
    tag,
    last_opened_at,
    SUBSTRING(TRIM(tag), 1, 4) AS tag_prefix,
    LENGTH(TRIM(tag)) AS tag_len
FROM per_group
WHERE tag = 'bug'
ORDER BY last_opened_at NULLS LAST
