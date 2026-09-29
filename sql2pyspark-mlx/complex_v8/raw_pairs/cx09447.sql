WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.score) AS prior_score_12h
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
combined_details_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        st.cohort AS student_cohort
    FROM in_window s
    LEFT JOIN students st ON st.id = s.student_id
),
grouped AS (
    -- Group-level totals
    SELECT student_id, COUNT(DISTINCT id) AS distinct_ids, COUNT(*) AS n_rows
    FROM combined_details_student
    GROUP BY 1
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    student_id,
    distinct_ids,
    n_rows,
    LEAST(n_rows, distinct_ids) AS least_n_rows_distinct_ids,
    LEAST(distinct_ids, n_rows) AS least_distinct_ids_n_rows
FROM grouped
ORDER BY n_rows DESC NULLS FIRST
