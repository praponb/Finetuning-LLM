WITH filtered AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        credits,
        level,
        LENGTH(TRIM(dept)) AS dept_len,
        LOWER(REGEXP_REPLACE(TRIM(course_code), ' ', '_')) AS course_code_slug
    FROM courses
    WHERE dept <> 'Bio'
      AND level IN ('intro', 'advanced')
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        dept_len,
        ROUND(AVG(credits), 2) AS avg_credits,
        ARRAY_SORT(COLLECT_SET(course_code)) AS course_code_list,
        SUM(credits) FILTER (WHERE course_code IS NOT NULL) AS credits_course_code_is_not_null,
        ARRAY_SORT(COLLECT_SET(level)) AS level_list,
        MIN(dept_len) AS min_dept_len
    FROM filtered
    GROUP BY dept_len
    HAVING SUM(credits) > 2
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY dept_len, min_dept_len ORDER BY min_dept_len DESC, dept_len) AS seq_no,
        ROUND(avg_credits / NULLIF(SUM(avg_credits) OVER w_dept_len_min_dept_len, 0), 4) AS avg_credits_share_of_dept_len_min_dept_len,
        ROUND(AVG(min_dept_len) OVER w_dept_len_min_dept_len, 2) AS dept_len_min_dept_len_avg_min_dept_len,
        SUM(min_dept_len) OVER w_dept_len_min_dept_len AS dept_len_min_dept_len_sum_min_dept_len
    FROM totals
    WINDOW w_dept_len_min_dept_len AS (PARTITION BY dept_len, min_dept_len)
),
derived AS (
    -- Derive helper columns
    SELECT
        dept_len,
        avg_credits,
        course_code_list,
        credits_course_code_is_not_null,
        min_dept_len,
        avg_credits_share_of_dept_len_min_dept_len,
        dept_len_min_dept_len_sum_min_dept_len,
        seq_no - min_dept_len AS seq_no_minus_min_dept_len,
        ROUND(dept_len / NULLIF(min_dept_len, 0), 3) AS dept_len_per_min_dept_len
    FROM sequenced
    WHERE min_dept_len >= 3
),
filtered_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM derived
    WHERE seq_no_minus_min_dept_len > (SELECT MIN(seq_no_minus_min_dept_len) FROM derived)
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered_2
    WHERE avg_credits_share_of_dept_len_min_dept_len < (SELECT MAX(avg_credits_share_of_dept_len_min_dept_len) FROM filtered_2)
)
-- Final projection
SELECT
    dept_len,
    course_code_list,
    credits_course_code_is_not_null,
    min_dept_len,
    dept_len_min_dept_len_sum_min_dept_len,
    dept_len_per_min_dept_len,
    LEAST(min_dept_len, avg_credits) AS least_min_dept_len_avg_credits
FROM without_matches
ORDER BY credits_course_code_is_not_null DESC
