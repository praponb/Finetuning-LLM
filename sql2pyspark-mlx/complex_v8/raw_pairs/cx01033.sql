WITH prepared AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM enrollments
    WHERE grade_status NOT IN ('FAIL', 'WITHDRAWN')
      AND score <= 69.6
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        term_prefix,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY MONTH(enrolled_at) ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(enrolled_at) ORDER BY enrolled_at, id), 3) AS enrolled_at_percent_rank
    FROM prepared
),
course_id_stats AS (
    -- Median enrolled_at_percent_rank per course_id
    SELECT course_id, PERCENTILE_APPROX(enrolled_at_percent_rank, 0.5) AS median_enrolled_at_percent_rank
    FROM with_windows
    GROUP BY course_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_enrolled_at_percent_rank,
        ROUND(s.enrolled_at_percent_rank - g.median_enrolled_at_percent_rank, 2) AS enrolled_at_percent_rank_minus_median
    FROM with_windows s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.enrolled_at_percent_rank >= g.median_enrolled_at_percent_rank
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        term_prefix,
        second_enrolled_at,
        enrolled_at_percent_rank,
        median_enrolled_at_percent_rank,
        enrolled_at_percent_rank_minus_median,
        LAST_VALUE(second_enrolled_at) IGNORE NULLS OVER (ORDER BY second_enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_second_enrolled_at,
        enrolled_at_percent_rank - LAG(enrolled_at_percent_rank) OVER (ORDER BY second_enrolled_at NULLS LAST, id) AS enrolled_at_percent_rank_change,
        ROUND(enrolled_at_percent_rank / NULLIF(SUM(enrolled_at_percent_rank) OVER (PARTITION BY course_id), 0), 4) AS enrolled_at_percent_rank_share_of_course_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at_percent_rank_minus_median, id), 3) AS enrolled_at_percent_rank_minus_median_percent_ra,
        ROW_NUMBER() OVER w_all_by_second_enrolled_at AS second_enrolled_at_seq
    FROM vs_group
    WINDOW w_all_by_second_enrolled_at AS (ORDER BY second_enrolled_at NULLS LAST, id)
),
tagged AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(term_prefix)) AS term_prefix_len
    FROM with_trends
    WHERE enrolled_at_percent_rank_minus_median < 0.2
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, term_prefix_len, second_enrolled_at_seq
    FROM tagged
) src
UNPIVOT (
    reading FOR measure IN (term_prefix_len, second_enrolled_at_seq)
) unp
