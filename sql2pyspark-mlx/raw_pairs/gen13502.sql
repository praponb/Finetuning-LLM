WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        location,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) event_seq,
        FIRST_VALUE(budget) IGNORE NULLS OVER w_ordered first_known_budget
    FROM departments
    WINDOW w_ordered AS (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        location,
        SUM(first_known_budget) OVER (PARTITION BY location ORDER BY first_known_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_first_known_budget,
        NTILE(5) OVER (ORDER BY first_known_budget DESC, id) first_known_budget_bucket,
        AVG(event_seq) OVER (ORDER BY first_known_budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) event_seq_avg_3,
        AVG(event_seq) OVER (PARTITION BY location ORDER BY event_seq, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) event_seq_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY first_known_budget DESC, id) seq_no
    FROM enriched
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY first_known_budget_bucket DESC, id) AS rn
    FROM scored
    QUALIFY rn = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(seq_no) IGNORE NULLS OVER (ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_seq_no,
        COUNT(cumulative_first_known_budget) OVER w_hist location_count_cumulative_first_known_budget,
        LEAD(cumulative_first_known_budget, 2) OVER (ORDER BY first_known_budget_bucket, id) next_cumulative_first_known_budget,
        SUM(event_seq_avg_3) OVER w_hist location_sum_event_seq_avg_3
    FROM deduped
    WHERE seq_no >= 5
    WINDOW w_hist AS (PARTITION BY location)
)
SELECT
    id,
    location,
    cumulative_first_known_budget,
    first_known_budget_bucket,
    event_seq_avg_3,
    seq_no,
    first_known_seq_no,
    location_count_cumulative_first_known_budget,
    next_cumulative_first_known_budget,
    CASE WHEN location RLIKE '^(Singapore|Berlin)$' THEN 'PRIORITY' WHEN location RLIKE '^(Bangkok|Chiang Mai)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS location_zone
FROM metrics
