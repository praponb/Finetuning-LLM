WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags
    FROM tickets
    WHERE csat >= 1
      AND priority = 'P3'
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        LENGTH(TRIM(channel)) AS channel_len,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text
    FROM cleaned_tickets
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        opened_at,
        closed_at,
        channel_len,
        channel_len - LAG(channel_len) OVER w_agent_id_opened_at_year_by_o AS channel_len_change,
        NTH_VALUE(channel_len, 2) OVER (PARTITION BY agent_id ORDER BY channel_len NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_channel_len,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY agent_id, YEAR(opened_at) ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        COUNT(*) OVER (PARTITION BY channel_len, agent_id) AS rows_in_channel_len_agent_id,
        ROUND(AVG(channel_len) OVER (PARTITION BY channel_len, agent_id), 2) AS channel_len_agent_id_avg_channel_len,
        ROUND(channel_len / NULLIF(SUM(channel_len) OVER w_channel_len_agent_id, 0), 4) AS channel_len_share_of_channel_len_agent_id
    FROM derived
    WINDOW w_agent_id_opened_at_year_by_o AS (PARTITION BY agent_id, YEAR(opened_at) ORDER BY opened_at, id), w_channel_len_agent_id AS (PARTITION BY channel_len, agent_id)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(channel_len_change / NULLIF(second_channel_len, 0), 3) AS channel_len_change_per_second_channel_len,
        COALESCE(second_channel_len, 0) AS second_channel_len_filled
    FROM sequenced
    WHERE rows_in_channel_len_agent_id >= 8
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        channel_len + second_channel_len AS channel_len_plus_second_channel_len,
        CASE WHEN channel_len_change >= 7 THEN 'A' WHEN channel_len_change >= -1 THEN 'B' ELSE 'C' END AS channel_len_change_tier
    FROM tagged
    WHERE TO_DATE(last_known_opened_at) < DATE '2025-03-08'
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        second_channel_len,
        TO_DATE(closed_at) AS closed_at_day,
        second_channel_len_filled,
        ROUND(SUM(channel_len_change_per_second_channel_len) / NULLIF(SUM(second_channel_len), 0), 3) AS channel_len_change_per_second_channel_len_to_sec,
        PERCENTILE_APPROX(channel_len_share_of_channel_len_agent_id, 0.5) AS median_channel_len_share_of_channel_len_agent_id,
        COUNT(DISTINCT id) AS distinct_ids
    FROM labeled
    GROUP BY second_channel_len, TO_DATE(closed_at), second_channel_len_filled
),
scored AS (
    -- Label and score rows
    SELECT
        second_channel_len,
        closed_at_day,
        second_channel_len_filled,
        channel_len_change_per_second_channel_len_to_sec,
        median_channel_len_share_of_channel_len_agent_id,
        distinct_ids,
        ABS(distinct_ids - 7) AS distinct_ids_distance,
        COALESCE(second_channel_len, 0) AS second_channel_len_filled_2,
        DATE_FORMAT(closed_at_day, 'yyyy-MM-dd') AS closed_at_day_label
    FROM rollup_base
)
SELECT
    second_channel_len,
    closed_at_day,
    second_channel_len_filled,
    distinct_ids,
    distinct_ids_distance,
    second_channel_len_filled_2,
    DATEDIFF(DATE '2025-06-30', closed_at_day) AS days_since_closed_at_day,
    SUBSTRING(TRIM(closed_at_day_label), 1, 4) AS closed_at_day_label_prefix
FROM scored
WHERE second_channel_len_filled_2 <= 12
ORDER BY closed_at_day
