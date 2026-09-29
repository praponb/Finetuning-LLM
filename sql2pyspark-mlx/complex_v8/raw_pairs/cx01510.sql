WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        COALESCE(score, 0.0) AS score_filled,
        COALESCE(score, 0.0) AS score_filled_2
    FROM enrollments
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(score_filled_2) OVER (ORDER BY score_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_filled_2_so_far,
        COUNT(*) OVER (PARTITION BY student_id, grade_status) AS rows_in_student_id_grade_status,
        FIRST_VALUE(score_filled) OVER (ORDER BY score_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score_filled,
        ROUND(AVG(score_filled) OVER (PARTITION BY student_id, course_id ORDER BY score_filled_2 ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS score_filled_avg_4,
        RANK() OVER w_student_id_by_score_ties AS score_rank,
        NTILE(5) OVER (PARTITION BY student_id, course_id ORDER BY score_filled_2 ASC NULLS LAST, id) AS score_filled_2_quintile,
        ROUND(score_filled - AVG(score_filled) OVER (PARTITION BY student_id, grade_status), 2) AS score_filled_vs_student_id_grade_status_avg
    FROM filtered
    WINDOW w_student_id_by_score_ties AS (PARTITION BY student_id ORDER BY score DESC)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM sequenced
    WHERE score_rank <= 5
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    score_filled_2,
    min_score_filled_2_so_far,
    rows_in_student_id_grade_status,
    first_score_filled,
    score_filled_avg_4,
    score_rank,
    score_filled_2_quintile,
    CASE WHEN score_filled_2 >= 65.0 THEN 'large' WHEN score_filled_2 >= 30.0 THEN 'medium' ELSE 'small' END AS score_filled_2_tier
FROM top_rows
WHERE student_id IS NOT NULL
ORDER BY rows_in_student_id_grade_status NULLS LAST
