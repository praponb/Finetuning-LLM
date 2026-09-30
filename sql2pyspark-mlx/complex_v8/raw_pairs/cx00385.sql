WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.csat) AS prior_csat_12h
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.agent_id, a.opened_at
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(prior_rows_12h) IGNORE NULLS OVER (ORDER BY prior_csat_12h NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prior_rows_12h,
        MIN(prior_csat_12h) OVER (PARTITION BY agent_id, prior_rows_12h) AS agent_id_prior_rows_12h_min_prior_csat_12h,
        ROW_NUMBER() OVER w_all_by_prior_csat_12h AS rn
    FROM covered
    WINDOW w_all_by_prior_csat_12h AS (ORDER BY prior_csat_12h NULLS LAST, id)
),
grouped AS (
    -- Summarise per group
    SELECT
        agent_id,
        first_known_prior_rows_12h,
        MIN(agent_id_prior_rows_12h_min_prior_csat_12h) AS min_agent_id_prior_rows_12h_min_prior_csat_12h,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_trends
    GROUP BY agent_id, first_known_prior_rows_12h
    HAVING SUM(agent_id_prior_rows_12h_min_prior_csat_12h) > 62
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(distinct_ids / 2) * 2 AS distinct_ids_bucket,
        FLOOR(distinct_ids / 2) * 2 AS distinct_ids_bucket_2,
        ABS(distinct_ids - 4) AS distinct_ids_distance
    FROM grouped
    WHERE agent_id <= 3
)
SELECT
    distinct_ids,
    distinct_ids_bucket_2,
    agent_id,
    GROUPING_ID() AS grouping_level,
    PERCENTILE_APPROX(first_known_prior_rows_12h, 0.5) AS median_first_known_prior_rows_12h,
    MAX(first_known_prior_rows_12h) AS max_first_known_prior_rows_12h
FROM derived
GROUP BY GROUPING SETS ((distinct_ids, distinct_ids_bucket_2, agent_id), (distinct_ids), (distinct_ids_bucket_2))
ORDER BY distinct_ids NULLS LAST, distinct_ids_bucket_2 NULLS LAST, agent_id NULLS LAST
