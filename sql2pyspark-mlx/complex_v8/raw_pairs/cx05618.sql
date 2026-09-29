WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        SUBSTRING(TRIM(grade_status), 1, 4) AS grade_status_prefix
    FROM enrollments
    WHERE course_id <= 9
      AND grade_status IS NOT NULL
),
totals AS (
    -- Summarise per group
    SELECT
        grade_status,
        COUNT(DISTINCT grade_status) AS distinct_grade_status,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        MIN(enrolled_at_month) AS first_enrolled_at_month
    FROM prepared
    WHERE grade_status_slug IS NOT NULL
    GROUP BY 1
    HAVING SUM(score) > 20.2
),
flattened AS (
    -- One row per grade_status_list_item
    SELECT
        s.grade_status,
        s.distinct_grade_status,
        s.row_count,
        s.term_list,
        s.first_enrolled_at_month,
        t.grade_status_list_item
    FROM totals s
    LATERAL VIEW EXPLODE(s.grade_status_list) t AS grade_status_list_item
),
tagged AS (
    -- Add calculated columns
    SELECT
        grade_status,
        distinct_grade_status,
        row_count,
        term_list,
        first_enrolled_at_month,
        grade_status_list_item,
        COALESCE(distinct_grade_status, 0) AS distinct_grade_status_filled,
        CONCAT(grade_status_list_item, '#', CAST(distinct_grade_status AS STRING)) AS grade_status_list_item_ref,
        DATE_FORMAT(first_enrolled_at_month, 'yyyy-MM-dd') AS first_enrolled_at_month_label
    FROM flattened
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY first_enrolled_at_month, grade_status, grade_status_list_item) AS first_enrolled_at_month_seq,
        LAG(first_enrolled_at_month, 3) OVER (PARTITION BY row_count, grade_status_list_item ORDER BY distinct_grade_status, grade_status, grade_status_list_item) AS prev3_first_enrolled_at_month,
        NTILE(4) OVER (ORDER BY first_enrolled_at_month, grade_status, grade_status_list_item) AS first_enrolled_at_month_quartile
    FROM tagged
)
-- Final projection
SELECT
    grade_status,
    distinct_grade_status,
    term_list,
    first_enrolled_at_month,
    grade_status_list_item,
    distinct_grade_status_filled,
    grade_status_list_item_ref,
    first_enrolled_at_month_seq,
    prev3_first_enrolled_at_month,
    first_enrolled_at_month_quartile
FROM windowed
ORDER BY distinct_grade_status DESC, grade_status_list_item
