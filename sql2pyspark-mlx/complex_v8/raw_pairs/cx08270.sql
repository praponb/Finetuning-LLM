WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.score) AS prior_score_3d
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
enriched AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.prior_rows_3d,
        st.cohort AS student_cohort
    FROM joined_ranges s
    JOIN students st ON st.id = s.student_id
    WHERE st.cohort = 'C2023'
),
summary AS (
    -- Group-level totals
    SELECT
        prior_rows_3d,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        ARRAY_SORT(COLLECT_SET(student_cohort)) AS student_cohort_list,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        ROUND(STDDEV(prior_rows_3d), 2) AS stddev_prior_rows_3d
    FROM enriched
    GROUP BY prior_rows_3d, DATE_TRUNC('MONTH', enrolled_at)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM summary
    WHERE distinct_student_ids > (SELECT AVG(distinct_student_ids) FROM summary)
)
SELECT
    prior_rows_3d,
    enrolled_at_month,
    student_cohort_list,
    distinct_student_ids,
    stddev_prior_rows_3d
FROM filtered
ORDER BY enrolled_at_month DESC, prior_rows_3d
LIMIT 20
