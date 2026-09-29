SELECT
    term,
    enrolled_at AS happened_at,
    score,
    'enrollment' AS origin
FROM enrollments
WHERE score < 58.8
  AND enrolled_at >= TIMESTAMP '2025-01-31 13:00:00'
UNION
SELECT
    grade_status AS term,
    enrolled_at AS happened_at,
    score,
    'enrollment_2' AS origin
FROM enrollments
WHERE grade_status NOT IN ('FAIL', 'INCOMPLETE')
  AND score > 20.6
