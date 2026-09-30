WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        city,
        signup_date,
        is_active,
        credit_limit,
        age,
        ROUND(age * 0.9, 2) AS age_adjusted
    FROM customers
    WHERE segment IS NOT NULL
      AND segment IS DISTINCT FROM 'SMB'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, city
    FROM filtered
    EXCEPT DISTINCT
    SELECT id, city
    FROM customers
    WHERE MONTH(signup_date) IN (3, 4, 6)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.city = k.city
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY age DESC NULLS FIRST) AS age_percent_rank,
        DENSE_RANK() OVER (PARTITION BY YEAR(signup_date) ORDER BY credit_limit NULLS FIRST) AS credit_limit_dense_rank,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC) AS credit_limit_dense_rank_2
    FROM reconciled
),
with_windows_customers AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(credit_limit_dense_rank / SUM(credit_limit_dense_rank) OVER (PARTITION BY city), 4) AS credit_limit_dense_rank_share,
        STDDEV_SAMP(credit_limit) OVER (PARTITION BY DATE_TRUNC('month', signup_date), city) AS signup_date_period_stddev_samp_credit_limit
    FROM with_windows
    WHERE credit_limit_dense_rank_2 > 6
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT_WS('/', city, email) AS city_email_key
    FROM with_windows_customers
    WHERE city IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', signup_date) AS week_start,
    city,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS credit_limit_per_london,
    ROUND(SUM(credit_limit_dense_rank_share) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS credit_limit_dense_rank_share_per_chiang_mai,
    GROUPING_ID() AS grouping_level
FROM base
GROUP BY CUBE (DATE_TRUNC('WEEK', signup_date), city)
HAVING credit_limit_dense_rank_share_per_chiang_mai >= 0
ORDER BY distinct_ids DESC NULLS FIRST;
