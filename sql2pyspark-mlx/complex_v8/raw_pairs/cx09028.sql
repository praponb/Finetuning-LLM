WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.score) AS prior_score_1d
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        enrolled_at,
        prior_score_1d,
        ABS(prior_score_1d - 150.0) AS prior_score_1d_distance,
        HOUR(enrolled_at) AS enrolled_at_hour,
        CASE WHEN prior_score_1d >= 300.0 THEN 'large' WHEN prior_score_1d >= 150.0 THEN 'medium' ELSE 'small' END AS prior_score_1d_tier
    FROM in_window
),
joined AS (
    -- Join students
    SELECT s.*, st.major AS student_major
    FROM annotated s
    JOIN students st ON st.id = s.student_id
    WHERE st.major = 'Math'
)
SELECT
    id,
    student_id,
    enrolled_at,
    prior_score_1d,
    prior_score_1d_distance,
    enrolled_at_hour,
    prior_score_1d_tier,
    student_major,
    DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
FROM joined
WHERE enrolled_at_hour <= 14
ORDER BY prior_score_1d_distance NULLS LAST, id DESC NULLS FIRST
