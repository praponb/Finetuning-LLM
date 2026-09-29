WITH prepared_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE requester_id <= 20
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        channel,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        csat,
        SUM(CASE WHEN channel = 'email' THEN csat ELSE 0 END) AS csat_channel_email,
        MIN(requester_id) AS min_requester_id,
        MAX(opened_at) AS last_opened_at,
        SUM(requester_id) AS total_requester_id,
        ROUND(SUM(csat) / NULLIF(SUM(requester_id), 0), 3) AS csat_to_requester_id_ratio,
        COUNT(DISTINCT id) AS distinct_ids
    FROM prepared_tickets
    WHERE requester_id > 5
    GROUP BY 1, 2, 3
    HAVING AVG(requester_id) > 7
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        channel,
        opened_at_month,
        csat,
        csat_channel_email,
        last_opened_at,
        total_requester_id,
        csat_to_requester_id_ratio,
        distinct_ids,
        MIN(min_requester_id) OVER w_channel AS channel_min_min_requester_id,
        ROUND(total_requester_id - AVG(total_requester_id) OVER (PARTITION BY channel), 2) AS total_requester_id_vs_channel_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat, MONTH(opened_at_month) ORDER BY total_requester_id DESC NULLS FIRST, channel, opened_at_month, csat), 3) AS total_requester_id_percent_rank
    FROM grouped
    WINDOW w_channel AS (PARTITION BY channel)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY total_requester_id_vs_channel_avg) AS total_requester_id_vs_channel_avg_dense_rank,
        NTH_VALUE(csat, 2) OVER (PARTITION BY distinct_ids, csat ORDER BY total_requester_id_percent_rank DESC, channel, opened_at_month, csat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat
    FROM windowed
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(second_csat, 0) AS second_csat_filled,
        ROUND(csat_channel_email / NULLIF(total_requester_id, 0), 3) AS csat_channel_email_per_total_requester_id
    FROM sequenced
    WHERE second_csat > 2
),
tagged AS (
    -- Derive helper columns
    SELECT
        channel,
        opened_at_month,
        csat,
        csat_channel_email,
        last_opened_at,
        total_requester_id,
        csat_to_requester_id_ratio,
        distinct_ids,
        total_requester_id_vs_channel_avg,
        second_csat,
        second_csat_filled,
        csat_channel_email_per_total_requester_id,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix
    FROM derived
    WHERE csat >= 3
)
SELECT
    second_csat,
    distinct_ids,
    SUM(second_csat) AS total_second_csat
FROM tagged
GROUP BY GROUPING SETS ((second_csat, distinct_ids), (second_csat), (distinct_ids), ())
ORDER BY second_csat NULLS LAST, distinct_ids NULLS LAST
