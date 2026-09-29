WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        GREATEST(requester_id, csat) AS greatest_requester_id_csat,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND ARRAY_CONTAINS(tags, 'refund')
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        channel,
        MIN(closed_at) AS first_closed_at,
        COUNT(DISTINCT id) AS distinct_ids
    FROM scoped
    WHERE channel_slug IS NOT NULL
    GROUP BY channel
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE distinct_ids > (SELECT AVG(distinct_ids) FROM stats)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        channel,
        distinct_ids,
        ROUND(AVG(distinct_ids) OVER (PARTITION BY distinct_ids ORDER BY first_closed_at DESC NULLS FIRST, channel ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS distinct_ids_avg_6_prior,
        LEAD(first_closed_at, 2) OVER (PARTITION BY distinct_ids ORDER BY first_closed_at DESC NULLS FIRST, channel) AS next2_first_closed_at,
        distinct_ids - LAG(distinct_ids) OVER (PARTITION BY distinct_ids ORDER BY first_closed_at DESC NULLS FIRST, channel) AS distinct_ids_change
    FROM filtered
),
windowed_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER w_next2_first_closed_at_to_dat AS rows_in_next2_first_closed_at_to_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_ids, channel ORDER BY distinct_ids_avg_6_prior), 3) AS distinct_ids_avg_6_prior_percent_rank,
        DENSE_RANK() OVER (PARTITION BY distinct_ids, channel ORDER BY distinct_ids_avg_6_prior, channel) AS distinct_ids_avg_6_prior_dense_rank,
        LAG(distinct_ids, 2) OVER (PARTITION BY distinct_ids ORDER BY distinct_ids_avg_6_prior DESC, channel) AS prev2_distinct_ids,
        DENSE_RANK() OVER w_distinct_ids_by_distinct_ids AS distinct_ids_avg_6_prior_dense_rank_2
    FROM windowed
    WINDOW w_next2_first_closed_at_to_dat AS (PARTITION BY TO_DATE(next2_first_closed_at)), w_distinct_ids_by_distinct_ids AS (PARTITION BY distinct_ids ORDER BY distinct_ids_avg_6_prior DESC, channel)
),
derived AS (
    -- Label and score rows
    SELECT *, UPPER(TRIM(channel)) AS channel_clean
    FROM windowed_2
)
SELECT
    channel,
    distinct_ids_avg_6_prior_dense_rank,
    PERCENTILE_APPROX(distinct_ids_avg_6_prior_dense_rank, 0.5) AS median_distinct_ids_avg_6_prior_dense_rank,
    ROUND(AVG(distinct_ids_avg_6_prior), 2) AS avg_distinct_ids_avg_6_prior
FROM derived
GROUP BY CUBE(channel, distinct_ids_avg_6_prior_dense_rank)
