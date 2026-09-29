WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        INITCAP(TRIM(term)) AS term_clean,
        CASE WHEN score >= 80.2 THEN 'A' WHEN score >= 36.6 THEN 'B' ELSE 'C' END AS score_tier,
        CONCAT_WS('|', term, grade_status) AS term_grade_status_label
    FROM enrollments
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        student_id,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE term IS NOT NULL) AS n_term_is_not_null,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        ROUND(100 * COUNT(CASE WHEN term IN ('2024T1', '2025T2') THEN 1 END) / COUNT(*), 1) AS pct_term_in_2024t1_2025t2
    FROM cleaned
    WHERE course_id <= 2
    GROUP BY student_id
),
scored AS (
    -- Add calculated columns
    SELECT
        student_id,
        term_list,
        ARRAY_JOIN(ARRAY_SORT(term_list), ',') AS term_list_text
    FROM per_group
),
enriched AS (
    -- Label and score rows
    SELECT
        student_id,
        term_list,
        term_list_text,
        LOWER(REGEXP_REPLACE(TRIM(term_list_text), ' ', '_')) AS term_list_text_slug
    FROM scored
    WHERE term_list_text IS NOT NULL
)
SELECT student_id, term_list_text, term_list_text_slug
FROM enriched
WHERE SIZE(term_list) >= 1
ORDER BY term_list_text, term_list_text_slug
