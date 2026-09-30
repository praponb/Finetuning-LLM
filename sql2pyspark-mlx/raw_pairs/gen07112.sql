WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN country = 'JP' THEN 1 ELSE 0 END is_jp,
        COALESCE(city, 'UNKNOWN') city_clean
    FROM customers
    WHERE signup_date > DATE '2023-09-19'
)
-- Running and moving aggregates
SELECT
    id,
    name,
    country,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    LAST_VALUE(city, TRUE) OVER w_hist last_known_city,
    DENSE_RANK() OVER w_grp is_jp_dense_rank,
    MIN(credit_limit) OVER (PARTITION BY country) country_min_credit_limit,
    ROW_NUMBER() OVER (ORDER BY age DESC, id) seq_no,
    SUM(is_jp) OVER (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_is_jp
FROM valid
WINDOW w_hist AS (PARTITION BY segment ORDER BY is_jp, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY country ORDER BY is_jp DESC NULLS LAST)
