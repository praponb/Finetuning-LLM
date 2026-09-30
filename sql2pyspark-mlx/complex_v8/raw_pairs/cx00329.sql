WITH prepared_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        credits,
        level
    FROM courses
    WHERE dept IS NOT NULL
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        level,
        credits,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(credits) AS min_credits,
        SUM(CASE WHEN level IN ('core', 'advanced') THEN 1 ELSE 0 END) AS n_level_in_core_advanced
    FROM prepared_courses
    WHERE level IS NOT NULL
    GROUP BY 1, 2
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(credits) OVER (PARTITION BY min_credits ORDER BY distinct_ids, level, credits ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_credits_so_far,
        ROUND(AVG(credits) OVER (PARTITION BY level ORDER BY distinct_ids, level, credits ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS credits_avg_2
    FROM summary
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM windowed
    WHERE distinct_ids > (SELECT AVG(distinct_ids) FROM windowed)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(distinct_ids - AVG(distinct_ids) OVER w_credits_min_credits, 2) AS distinct_ids_vs_credits_min_credits_avg,
        LEAD(n_level_in_core_advanced, 2) OVER w_min_credits_level_by_min_cre AS next2_n_level_in_core_advanced,
        NTH_VALUE(credits, 2) OVER (PARTITION BY min_credits, level ORDER BY min_credits ASC, level, credits ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_credits,
        ROUND(CUME_DIST() OVER (PARTITION BY min_credits, level ORDER BY min_credits ASC), 3) AS min_credits_cume_dist,
        MAX(n_level_in_core_advanced) OVER (PARTITION BY min_credits, level ORDER BY min_credits ASC, level, credits ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS n_level_in_core_advanced_max_6_prior,
        MIN(min_credits) OVER (PARTITION BY credits, min_credits) AS credits_min_credits_min_min_credits
    FROM without_matches
    WINDOW w_credits_min_credits AS (PARTITION BY credits, min_credits), w_min_credits_level_by_min_cre AS (PARTITION BY min_credits, level ORDER BY min_credits ASC, level, credits)
),
tagged AS (
    -- Label and score rows
    SELECT
        level,
        credits,
        distinct_ids,
        min_credits,
        n_level_in_core_advanced,
        min_credits_so_far,
        distinct_ids_vs_credits_min_credits_avg,
        next2_n_level_in_core_advanced,
        second_credits,
        min_credits_cume_dist,
        n_level_in_core_advanced_max_6_prior,
        credits_min_credits_min_min_credits,
        CASE WHEN distinct_ids >= 9 THEN 'A' WHEN distinct_ids >= 4 THEN 'B' ELSE 'C' END AS distinct_ids_tier
    FROM metrics
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        level,
        credits,
        distinct_ids,
        min_credits,
        min_credits_so_far,
        distinct_ids_vs_credits_min_credits_avg,
        next2_n_level_in_core_advanced,
        second_credits,
        credits_min_credits_min_min_credits,
        distinct_ids_tier,
        COUNT(*) OVER w_distinct_ids AS rows_in_distinct_ids,
        ROUND(AVG(credits) OVER (ORDER BY second_credits, level, credits ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS credits_avg_1_prior,
        NTH_VALUE(n_level_in_core_advanced, 2) OVER (ORDER BY second_credits, level, credits ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_level_in_core_advanced,
        MIN(credits) OVER w_distinct_ids AS distinct_ids_min_credits
    FROM tagged
    WINDOW w_distinct_ids AS (PARTITION BY distinct_ids)
)
-- Final projection
SELECT
    level,
    credits,
    min_credits,
    min_credits_so_far,
    next2_n_level_in_core_advanced,
    credits_min_credits_min_min_credits,
    distinct_ids_tier,
    credits_avg_1_prior,
    distinct_ids_min_credits
FROM sequenced
ORDER BY min_credits DESC NULLS FIRST
