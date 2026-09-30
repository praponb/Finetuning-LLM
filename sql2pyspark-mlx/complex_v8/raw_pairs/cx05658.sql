WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-12 09:00:00'
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_ym,
        MIN(enrolled_at) AS first_enrolled_at,
        COUNT(DISTINCT id) AS distinct_ids
    FROM valid
    WHERE student_id <= 13
    GROUP BY DATE_FORMAT(enrolled_at, 'yyyy-MM')
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(distinct_ids) OVER (PARTITION BY distinct_ids, TO_DATE(first_enrolled_at)) AS distinct_ids_first_enrolled_at_to_date_sum_disti,
        LEAD(first_enrolled_at) OVER (PARTITION BY distinct_ids ORDER BY first_enrolled_at DESC, enrolled_at_ym) AS next_first_enrolled_at,
        LAG(distinct_ids) OVER w_distinct_ids_by_first_enroll AS prev_distinct_ids,
        MAX(distinct_ids) OVER (PARTITION BY distinct_ids ORDER BY first_enrolled_at DESC, enrolled_at_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_distinct_ids_so_far,
        SUM(distinct_ids) OVER (PARTITION BY distinct_ids ORDER BY distinct_ids DESC, enrolled_at_ym ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS distinct_ids_sum_3
    FROM grouped
    WINDOW w_distinct_ids_by_first_enroll AS (PARTITION BY distinct_ids ORDER BY first_enrolled_at DESC, enrolled_at_ym)
)
SELECT enrolled_at_ym, metric, reading
FROM (
    SELECT enrolled_at_ym, distinct_ids_first_enrolled_at_to_date_sum_disti, distinct_ids, distinct_ids_sum_3, prev_distinct_ids
    FROM sequenced
) src
UNPIVOT (
    reading FOR metric IN (distinct_ids_first_enrolled_at_to_date_sum_disti, distinct_ids, distinct_ids_sum_3, prev_distinct_ids)
) unp
