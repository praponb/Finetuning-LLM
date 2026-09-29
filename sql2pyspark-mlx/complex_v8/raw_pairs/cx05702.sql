WITH base AS (
    -- Filter listeners
    SELECT
        id,
        username,
        signup_date,
        birth_year,
        interests,
        DATE_FORMAT(signup_date, 'yyyy-MM-dd') AS signup_date_label,
        CASE WHEN birth_year >= 1981 THEN 'HIGH' WHEN birth_year >= 1975 THEN 'MEDIUM' ELSE 'LOW' END AS birth_year_tier
    FROM listeners
    WHERE birth_year <= 1994
      AND country <> 'JP'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_date_ym,
        COUNT(*) AS cnt,
        SUM(birth_year) AS total_birth_year,
        SUM(birth_year) FILTER (WHERE UPPER(TRIM(username)) LIKE 'C%') AS birth_year_upper_trim_username_like_c,
        MIN(signup_date) AS first_signup_date
    FROM base
    WHERE signup_date_label IS NOT NULL
    GROUP BY 1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        signup_date_ym,
        cnt,
        total_birth_year,
        birth_year_upper_trim_username_like_c,
        LEAD(total_birth_year, 2, 0) OVER (PARTITION BY cnt, DATE_TRUNC('MONTH', first_signup_date) ORDER BY total_birth_year DESC NULLS FIRST, signup_date_ym) AS next2_total_birth_year,
        ROW_NUMBER() OVER (PARTITION BY cnt, DATE_TRUNC('MONTH', first_signup_date) ORDER BY total_birth_year DESC NULLS FIRST, signup_date_ym) AS rn
    FROM stats
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(total_birth_year / NULLIF(rn, 0), 3) AS total_birth_year_per_rn,
        LEAST(total_birth_year, next2_total_birth_year) AS least_total_birth_year_next2_total_birth_year,
        SUBSTRING(TRIM(signup_date_ym), 1, 2) AS signup_date_ym_prefix
    FROM windowed
    WHERE rn >= 3
)
-- Subtotals with GROUPING SETS
SELECT
    cnt,
    rn,
    MIN(least_total_birth_year_next2_total_birth_year) AS min_least_total_birth_year_next2_total_birth_yea
FROM scored
GROUP BY GROUPING SETS ((cnt, rn), (cnt), (rn), ())
ORDER BY cnt NULLS LAST, rn NULLS LAST
