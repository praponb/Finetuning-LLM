WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        COALESCE(credits, 0) AS credits_filled,
        ABS(credits - 2) AS credits_distance
    FROM courses
    WHERE dept IN ('Bio', 'Math')
      AND level = 'core'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        credits_filled,
        credits_distance,
        MIN(credits_filled) OVER (PARTITION BY credits, credits_filled) AS credits_credits_filled_min_credits_filled,
        NTH_VALUE(credits_distance, 2) OVER (PARTITION BY credits_filled, dept ORDER BY credits_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_credits_distance,
        SUM(credits_filled) OVER (PARTITION BY dept, credits ORDER BY credits, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS credits_filled_sum_1_prior,
        ROW_NUMBER() OVER w_credits_filled_credits_dista AS credits_distance_seq,
        MAX(credits) OVER (PARTITION BY credits, credits_filled) AS credits_credits_filled_max_credits,
        LAG(credits) OVER (PARTITION BY dept ORDER BY credits_distance, id) AS prev_credits,
        ROUND(credits / NULLIF(SUM(credits) OVER (PARTITION BY credits), 0), 4) AS credits_share_of_credits,
        ROUND(AVG(credits_distance) OVER (PARTITION BY dept, credits ORDER BY credits, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS credits_distance_avg_3_prior,
        LAST_VALUE(credits_filled) IGNORE NULLS OVER (PARTITION BY dept, credits ORDER BY credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credits_filled,
        DENSE_RANK() OVER (PARTITION BY credits_filled, credits_distance ORDER BY credits ASC) AS credits_dense_rank,
        ROW_NUMBER() OVER w_dept_credits_by_credits AS seq_no,
        LAG(credits_filled, 3) OVER (PARTITION BY credits_filled, dept ORDER BY credits_filled, id) AS prev3_credits_filled,
        COUNT(*) OVER (PARTITION BY credits) AS rows_in_credits,
        ROUND(credits / NULLIF(SUM(credits) OVER (PARTITION BY credits, credits_filled), 0), 4) AS credits_share_of_credits_credits_filled
    FROM scoped
    WINDOW w_credits_filled_credits_dista AS (PARTITION BY credits_filled, credits_distance ORDER BY credits_distance, id), w_dept_credits_by_credits AS (PARTITION BY dept, credits ORDER BY credits, id)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        course_code,
        credits,
        level,
        credits_filled,
        credits_credits_filled_min_credits_filled,
        credits_filled_sum_1_prior,
        credits_distance_seq,
        credits_share_of_credits,
        credits_distance_avg_3_prior,
        credits_dense_rank,
        seq_no,
        prev3_credits_filled,
        rows_in_credits,
        credits_share_of_credits_credits_filled,
        ROUND(credits_share_of_credits * 0.25, 2) AS credits_share_of_credits_adj,
        LENGTH(TRIM(level)) AS level_len,
        COALESCE(level, 'UNKNOWN') AS level_or_unknown
    FROM ranked
    WHERE second_credits_distance > 1
)
-- Final projection
SELECT
    id,
    course_code,
    credits,
    level,
    credits_filled,
    credits_credits_filled_min_credits_filled,
    credits_filled_sum_1_prior,
    credits_share_of_credits,
    credits_distance_avg_3_prior,
    credits_dense_rank,
    seq_no,
    prev3_credits_filled,
    rows_in_credits,
    credits_share_of_credits_credits_filled,
    credits_share_of_credits_adj,
    level_len
FROM derived
WHERE level_len >= 3
ORDER BY level_len, credits NULLS LAST, id
LIMIT 10
