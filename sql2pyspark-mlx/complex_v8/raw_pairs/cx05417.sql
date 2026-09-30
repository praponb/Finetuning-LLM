WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek
    FROM enrollments
    WHERE score <= 56.1
      AND course_id <= 8
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(enrolled_at_dayofweek) OVER (ORDER BY enrolled_at NULLS LAST, id) AS prev_enrolled_at_dayofweek,
        NTILE(4) OVER w_enrolled_at_dayofweek_by_enr AS enrolled_at_quartile,
        FIRST_VALUE(enrolled_at_dayofweek) OVER (ORDER BY enrolled_at_dayofweek DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at_dayofweek,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY term)) AS distinct_grade_status_in_term,
        SUM(enrolled_at_dayofweek) OVER (ORDER BY enrolled_at_dayofweek DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_enrolled_at_dayofweek,
        ROUND(AVG(enrolled_at_dayofweek) OVER (PARTITION BY term), 2) AS term_avg_enrolled_at_dayofweek,
        ROUND(AVG(enrolled_at_dayofweek) OVER (PARTITION BY enrolled_at_dayofweek ORDER BY enrolled_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS enrolled_at_dayofweek_avg_3,
        RANK() OVER w_all_by_enrolled_at AS enrolled_at_rank,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        NTILE(4) OVER (ORDER BY enrolled_at NULLS LAST, id) AS enrolled_at_quartile_2
    FROM prepared_enrollments
    WINDOW w_enrolled_at_dayofweek_by_enr AS (PARTITION BY enrolled_at_dayofweek ORDER BY enrolled_at ASC, id), w_all_by_enrolled_at AS (ORDER BY enrolled_at NULLS LAST, id)
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM sequenced
    WHERE enrolled_at_rank <= 2
      AND first_enrolled_at_dayofweek > 2
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        enrolled_at_quartile,
        first_enrolled_at_dayofweek,
        running_enrolled_at_dayofweek,
        term_avg_enrolled_at_dayofweek,
        enrolled_at_dayofweek_avg_3,
        rows_in_term,
        rows_in_term - LAG(rows_in_term) OVER w_student_id_first_enrolled_at AS rows_in_term_change,
        first_enrolled_at_dayofweek - LAG(first_enrolled_at_dayofweek) OVER w_term_by_enrolled_at AS first_enrolled_at_dayofweek_change,
        rows_in_term - LAG(rows_in_term) OVER w_term_by_enrolled_at AS rows_in_term_change_2,
        NTH_VALUE(first_enrolled_at_dayofweek, 2) OVER (PARTITION BY student_id, first_enrolled_at_dayofweek ORDER BY prev_enrolled_at_dayofweek DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_enrolled_at_dayofweek,
        LAG(enrolled_at_dayofweek, 3) OVER (PARTITION BY term ORDER BY enrolled_at DESC, id) AS prev3_enrolled_at_dayofweek
    FROM leaders
    WINDOW w_student_id_first_enrolled_at AS (PARTITION BY student_id, first_enrolled_at_dayofweek ORDER BY prev_enrolled_at_dayofweek DESC, id), w_term_by_enrolled_at AS (PARTITION BY term ORDER BY enrolled_at DESC, id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        second_first_enrolled_at_dayofweek,
        SUM(CASE WHEN term IS NOT NULL THEN rows_in_term_change_2 ELSE 0 END) AS rows_in_term_change_2_term_is_not_null,
        ROUND(SUM(second_first_enrolled_at_dayofweek) / NULLIF(SUM(term_avg_enrolled_at_dayofweek), 0), 3) AS second_first_enrolled_at_dayofweek_to_term_avg_e,
        COUNT(DISTINCT id) AS distinct_ids
    FROM ranked
    WHERE prev3_enrolled_at_dayofweek > 3
    GROUP BY second_first_enrolled_at_dayofweek
    HAVING MAX(first_enrolled_at_dayofweek) > 3
)
SELECT
    second_first_enrolled_at_dayofweek,
    rows_in_term_change_2_term_is_not_null,
    second_first_enrolled_at_dayofweek_to_term_avg_e,
    FLOOR(distinct_ids / 2) * 2 AS distinct_ids_bucket,
    ABS(distinct_ids - 6) AS distinct_ids_distance
FROM stats
ORDER BY second_first_enrolled_at_dayofweek_to_term_avg_e DESC
