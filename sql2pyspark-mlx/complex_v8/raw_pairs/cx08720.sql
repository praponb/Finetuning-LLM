WITH matched AS (
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
latest AS (
    -- Keep the latest row per student_id
    SELECT
        id,
        student_id,
        enrolled_at,
        prior_rows_1d,
        prior_score_1d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY prior_rows_1d DESC, id DESC) AS row_num
        FROM matched
    ) ranked
    WHERE row_num = 1
),
combined_details AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.prior_rows_1d,
        s.prior_score_1d,
        st.cohort AS student_cohort,
        st.student_name,
        st.gpa AS student_gpa
    FROM latest s
    JOIN students st ON st.id = s.student_id AND st.enrolled_on IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(prior_rows_1d) IGNORE NULLS OVER (PARTITION BY student_cohort ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prior_rows_1d,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY student_cohort ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        NTH_VALUE(prior_score_1d, 2) OVER (PARTITION BY prior_rows_1d, student_id ORDER BY prior_score_1d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prior_score_1d,
        ROW_NUMBER() OVER w_prior_rows_1d_student_id_by_2 AS rn,
        COUNT(*) OVER (PARTITION BY MONTH(enrolled_at), student_id) AS rows_in_enrolled_at_month_student_id
    FROM combined_details
    WINDOW w_prior_rows_1d_student_id_by_2 AS (PARTITION BY prior_rows_1d, student_id ORDER BY prior_score_1d DESC, id)
)
SELECT id, metric, reading
FROM (
    SELECT id, rows_in_enrolled_at_month_student_id, prior_rows_1d
    FROM windowed
) src
UNPIVOT (
    reading FOR metric IN (rows_in_enrolled_at_month_student_id AS m_rows_in_enrolled_at_month_student_id, prior_rows_1d AS prior_rows_1d_v)
) unp
