WITH scoped_enrollments AS (
    -- Scope the enrollments rows
    SELECT *
    FROM enrollments
)
SELECT grade_status, enrolled_at AS happened_at
FROM scoped_enrollments
UNION
SELECT term AS grade_status, enrolled_at AS happened_at
FROM enrollments
ORDER BY happened_at, grade_status
