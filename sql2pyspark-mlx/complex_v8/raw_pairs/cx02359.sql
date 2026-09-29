WITH covered AS (
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
    -- Join students
    SELECT
        s.*,
        st.student_name,
        st.enrolled_on AS student_enrolled_on,
        st.gpa AS student_gpa
    FROM covered s
    LEFT JOIN students st ON st.id = s.student_id
),
combined_details_student_2 AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.prior_score_12h,
        s.student_enrolled_on,
        s.student_gpa,
        st.enrolled_on AS student_enrolled_on_2,
        st.cohort AS student_cohort
    FROM combined_details_student s
    JOIN students st ON st.id = s.student_id AND st.cohort IN ('C2023', 'C2021')
    WHERE st.enrolled_on < DATE '2025-07-18'
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        prior_score_12h,
        student_enrolled_on,
        student_gpa,
        student_enrolled_on_2,
        student_cohort,
        COALESCE(student_cohort, 'UNKNOWN') AS student_cohort_or_unknown,
        DATEDIFF(DATE '2025-06-30', student_enrolled_on) AS days_since_student_enrolled_on
    FROM combined_details_student_2
    WHERE prior_score_12h >= 175.0
)
SELECT
    id,
    prior_score_12h,
    student_enrolled_on,
    student_gpa,
    student_cohort_or_unknown,
    days_since_student_enrolled_on,
    CASE WHEN student_gpa >= 3.3 THEN 'HIGH' WHEN student_gpa >= 2.7 THEN 'MEDIUM' ELSE 'LOW' END AS student_gpa_tier
FROM enriched
