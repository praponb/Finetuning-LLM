WITH cleaned AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        INITCAP(TRIM(dept)) AS dept_clean
    FROM courses
    WHERE dept = 'Bio'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        dept,
        credits,
        level,
        dept_clean,
        FIRST_VALUE(credits) IGNORE NULLS OVER (PARTITION BY level ORDER BY credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credits,
        ROUND(AVG(credits) OVER (PARTITION BY credits, dept ORDER BY credits, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS credits_avg_1_prior,
        LAST_VALUE(credits) OVER (PARTITION BY credits, dept ORDER BY credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_credits,
        ROUND(credits / NULLIF(SUM(credits) OVER (PARTITION BY credits, level), 0), 4) AS credits_share_of_credits_level,
        MAX(credits) OVER (PARTITION BY credits, level) AS credits_level_max_credits,
        NTH_VALUE(credits, 2) OVER (PARTITION BY level ORDER BY credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_credits,
        LEAD(credits) OVER (PARTITION BY credits, dept ORDER BY credits, id) AS next_credits,
        ROUND(credits - AVG(credits) OVER (PARTITION BY credits, level), 2) AS credits_vs_credits_level_avg,
        ROUND(AVG(credits) OVER (PARTITION BY credits, level), 2) AS credits_level_avg_credits,
        SUM(credits) OVER (PARTITION BY credits, dept ORDER BY credits DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS credits_sum_3_prior
    FROM cleaned
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(dept_clean), ' ', '_')) AS dept_clean_slug,
        CONCAT(dept, '#', CAST(first_known_credits AS STRING)) AS dept_ref,
        ROUND(credits_sum_3_prior / NULLIF(first_known_credits, 0), 3) AS credits_sum_3_prior_per_first_known_credits
    FROM with_trends
)
SELECT
    id,
    dept,
    credits,
    level,
    dept_clean,
    first_known_credits,
    credits_avg_1_prior,
    last_credits,
    credits_share_of_credits_level,
    credits_level_max_credits,
    second_credits,
    next_credits,
    credits_vs_credits_level_avg,
    credits_level_avg_credits,
    credits_sum_3_prior,
    dept_ref,
    credits_sum_3_prior_per_first_known_credits,
    SUBSTRING(TRIM(dept_clean_slug), 1, 2) AS dept_clean_slug_prefix,
    LEAST(credits_sum_3_prior, credits) AS least_credits_sum_3_prior_credits
FROM enriched
WHERE credits_vs_credits_level_avg >= -2.0
ORDER BY credits_avg_1_prior DESC
