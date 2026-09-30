WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        TO_DATE(enrolled_at) AS enrolled_at_date,
        CASE WHEN term IN ('2025T2', '2025T1') THEN 'group_a' WHEN term = '2024T1' THEN 'group_b' ELSE 'other' END AS term_group
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 10 AND 16
),
grouped AS (
    -- Summarise per group
    SELECT
        term,
        course_id,
        MIN(enrolled_at_date) AS first_enrolled_at_date,
        ROUND(AVG(score), 2) AS avg_score,
        COUNT(*) AS cnt,
        COUNT(DISTINCT term_group) AS distinct_term_groups,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        MAX(enrolled_at) AS last_enrolled_at
    FROM filtered_enrollments
    GROUP BY 1, 2
    HAVING COUNT(*) >= 3
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        term,
        course_id,
        first_enrolled_at_date,
        avg_score,
        distinct_term_groups,
        last_enrolled_at,
        LEAD(last_enrolled_at) OVER (PARTITION BY term ORDER BY first_enrolled_at_date, term, course_id) AS next_last_enrolled_at,
        COUNT(*) OVER w_distinct_term_groups_course AS rows_in_distinct_term_groups_course_id
    FROM grouped
    WINDOW w_distinct_term_groups_course AS (PARTITION BY distinct_term_groups, course_id)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        term,
        course_id,
        first_enrolled_at_date,
        avg_score,
        distinct_term_groups,
        last_enrolled_at,
        next_last_enrolled_at,
        rows_in_distinct_term_groups_course_id,
        DENSE_RANK() OVER w_distinct_term_groups_by_next AS next_last_enrolled_at_dense_rank,
        NTILE(4) OVER (PARTITION BY MONTH(last_enrolled_at) ORDER BY avg_score, term, course_id) AS avg_score_quartile
    FROM windowed
    WINDOW w_distinct_term_groups_by_next AS (PARTITION BY distinct_term_groups ORDER BY next_last_enrolled_at, term, course_id)
),
screened AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND c.dept = 'Biz'
        )
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND MONTH(e.enrolled_at) IN (1, 2)
        )
      AND s.avg_score_quartile BETWEEN 3 AND 8
)
SELECT term, course_id, measure, amount
FROM (
    SELECT term, course_id, next_last_enrolled_at_dense_rank, rows_in_distinct_term_groups_course_id
    FROM qualified
) src
UNPIVOT (
    amount FOR measure IN (next_last_enrolled_at_dense_rank, rows_in_distinct_term_groups_course_id)
) unp
