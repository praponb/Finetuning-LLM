WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 79.3 THEN 'HIGH' WHEN score >= 23.7 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
    WHERE term IN ('2025T1', '2024T2', '2025T2')
      AND course_id IS NOT NULL
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        INITCAP(TRIM(score_tier)) AS score_tier_clean,
        CASE WHEN score >= 66.0 THEN 'large' WHEN score >= 23.7 THEN 'medium' ELSE 'small' END AS score_tier_2
    FROM recent
    WHERE course_id IS NOT NULL
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        score_tier_2,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list
    FROM enriched
    GROUP BY DATE_TRUNC('MONTH', enrolled_at), score_tier_2
),
exploded AS (
    -- One row per term_list_item
    SELECT
        s.enrolled_at_month,
        s.score_tier_2,
        s.cnt,
        s.grade_status_list,
        t.term_list_item_pos,
        t.term_list_item
    FROM per_group s
    LATERAL VIEW POSEXPLODE(s.term_list) t AS term_list_item_pos, term_list_item
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        enrolled_at_month,
        score_tier_2,
        cnt,
        term_list_item_pos,
        SUM(term_list_item_pos) OVER (PARTITION BY cnt) AS cnt_sum_term_list_item_pos,
        MAX(cnt) OVER (PARTITION BY cnt) AS cnt_max_cnt,
        ROUND(term_list_item_pos - AVG(term_list_item_pos) OVER (PARTITION BY cnt), 2) AS term_list_item_pos_vs_cnt_avg,
        ROW_NUMBER() OVER w_score_tier_2_term_list_item AS cnt_seq
    FROM exploded
    WINDOW w_score_tier_2_term_list_item AS (PARTITION BY score_tier_2, term_list_item ORDER BY cnt NULLS LAST, enrolled_at_month, score_tier_2, term_list_item_pos)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM windowed
    WHERE cnt_sum_term_list_item_pos > (SELECT AVG(cnt_sum_term_list_item_pos) FROM windowed)
)
SELECT
    cnt_seq,
    cnt,
    term_list_item_pos,
    GROUPING(cnt_seq) AS is_total_cnt_seq,
    MAX(enrolled_at_month) AS last_enrolled_at_month
FROM screened
GROUP BY ROLLUP(cnt_seq, cnt, term_list_item_pos)
ORDER BY cnt_seq NULLS LAST, cnt NULLS LAST, term_list_item_pos NULLS LAST
