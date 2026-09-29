WITH filtered AS (
    -- Filter students
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        gpa,
        is_international,
        COALESCE(gpa, 0.0) AS gpa_filled,
        MONTH(enrolled_on) AS enrolled_on_month,
        CONCAT_WS(' / ', major, student_name) AS major_student_name_label
    FROM students
    WHERE (major IS NOT NULL OR is_international = TRUE)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        enrolled_on,
        gpa,
        gpa_filled,
        ROUND(PERCENT_RANK() OVER (ORDER BY gpa ASC, id), 3) AS gpa_percent_rank,
        ROUND(gpa_filled - AVG(gpa_filled) OVER (PARTITION BY is_international), 2) AS gpa_filled_vs_is_international_avg,
        ROUND(gpa / NULLIF(SUM(gpa) OVER w_is_international, 0), 4) AS gpa_share_of_is_international,
        NTILE(5) OVER w_all_by_gpa AS gpa_quintile,
        ROUND(enrolled_on_month / NULLIF(SUM(enrolled_on_month) OVER (PARTITION BY is_international), 0), 4) AS enrolled_on_month_share_of_is_international,
        DENSE_RANK() OVER (ORDER BY gpa ASC, id) AS gpa_dense_rank
    FROM filtered
    WINDOW w_is_international AS (PARTITION BY is_international), w_all_by_gpa AS (ORDER BY gpa ASC, id)
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        enrolled_on,
        gpa,
        gpa_filled,
        gpa_percent_rank,
        gpa_filled_vs_is_international_avg,
        gpa_quintile,
        enrolled_on_month_share_of_is_international,
        gpa_dense_rank,
        ROW_NUMBER() OVER w_gpa_dense_rank_by_enrolled_o AS enrolled_on_seq,
        RANK() OVER w_gpa_dense_rank_by_enrolled_o_2 AS enrolled_on_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY gpa_quintile, gpa_dense_rank ORDER BY gpa_filled DESC, id), 3) AS gpa_filled_cume_dist,
        ROUND(AVG(gpa) OVER (PARTITION BY gpa_dense_rank ORDER BY enrolled_on, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS gpa_avg_2,
        COUNT(*) OVER (PARTITION BY gpa_dense_rank) AS rows_in_gpa_dense_rank
    FROM sequenced
    WINDOW w_gpa_dense_rank_by_enrolled_o AS (PARTITION BY gpa_dense_rank ORDER BY enrolled_on, id), w_gpa_dense_rank_by_enrolled_o_2 AS (PARTITION BY gpa_dense_rank ORDER BY enrolled_on)
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM sequenced_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.score < 71.0
              AND e.grade_status = 'INCOMPLETE'
        )
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(gpa_filled * 1.5, 2) AS gpa_filled_adj,
        DAYOFWEEK(enrolled_on) AS enrolled_on_dayofweek
    FROM screened
)
-- Final projection
SELECT DISTINCT enrolled_on_rank, gpa_dense_rank
FROM labeled
ORDER BY enrolled_on_rank DESC
