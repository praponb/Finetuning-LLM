WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat,
        LENGTH(TRIM(priority)) AS priority_len,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM tickets
    WHERE requester_id > 7
      AND csat >= 3
),
totals AS (
    -- Summarise per group
    SELECT
        priority_len,
        priority,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        ROUND(SUM(requester_id) / NULLIF(SUM(priority_len), 0), 3) AS requester_id_to_priority_len_ratio,
        COUNT(*) AS cnt,
        MIN(requester_id) AS min_requester_id
    FROM recent
    WHERE HOUR(closed_at) BETWEEN 10 AND 14
    GROUP BY 1, 2, 3
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM totals
    WHERE requester_id_to_priority_len_ratio > (SELECT AVG(requester_id_to_priority_len_ratio) FROM totals)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        priority_len,
        priority,
        opened_at_month,
        channel_list,
        requester_id_to_priority_len_ratio,
        cnt,
        min_requester_id,
        NTH_VALUE(priority_len, 2) OVER (PARTITION BY priority_len, cnt ORDER BY requester_id_to_priority_len_ratio DESC, priority_len, priority, opened_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_priority_len,
        COUNT(*) OVER w_priority_priority_len AS rows_in_priority_priority_len,
        FIRST_VALUE(cnt) IGNORE NULLS OVER (PARTITION BY priority_len, cnt ORDER BY requester_id_to_priority_len_ratio DESC, priority_len, priority, opened_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cnt
    FROM qualified
    WINDOW w_priority_priority_len AS (PARTITION BY priority, priority_len)
),
labeled AS (
    -- Label and score rows
    SELECT
        priority_len,
        priority,
        opened_at_month,
        channel_list,
        min_requester_id,
        second_priority_len,
        first_known_cnt,
        DAYOFWEEK(opened_at_month) AS opened_at_month_dayofweek
    FROM metrics
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        priority_len,
        priority,
        opened_at_month,
        channel_list,
        min_requester_id,
        second_priority_len,
        first_known_cnt,
        opened_at_month_dayofweek,
        ROUND(min_requester_id / NULLIF(SUM(min_requester_id) OVER (PARTITION BY second_priority_len), 0), 4) AS min_requester_id_share_of_second_priority_len,
        ROUND(AVG(opened_at_month_dayofweek) OVER (PARTITION BY second_priority_len), 2) AS second_priority_len_avg_opened_at_month_dayofwee,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY second_priority_len)) AS distinct_priority_in_second_priority_len,
        SUM(first_known_cnt) OVER w_second_priority_len AS second_priority_len_sum_first_known_cnt
    FROM labeled
    WINDOW w_second_priority_len AS (PARTITION BY second_priority_len)
)
-- Native SQL UNPIVOT (wide to long)
SELECT priority_len, priority, field, reading
FROM (
    SELECT priority_len, priority, opened_at_month_dayofweek, second_priority_len, min_requester_id, distinct_priority_in_second_priority_len
    FROM metrics_2
) src
UNPIVOT (
    reading FOR field IN (opened_at_month_dayofweek AS m_opened_at_month_dayofweek, second_priority_len AS m_second_priority_len, min_requester_id AS min_requester_id_v, distinct_priority_in_second_priority_len AS m_distinct_priority_in_second_priority_len)
) unp
