WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(is_active AS INT) AS is_active_flag,
        REGEXP_EXTRACT(email, '@(.+)$', 1) AS email_domain,
        COALESCE(segment, 'N/A') AS resolved_segment
    FROM customers
    WHERE segment IS NOT DISTINCT FROM 'Corporate'
      AND city IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(age) OVER (PARTITION BY city ORDER BY age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_age,
        MIN(age) OVER (PARTITION BY city ORDER BY age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_age,
        SUM(age) OVER (PARTITION BY city ORDER BY age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_age,
        COUNT(age) OVER (PARTITION BY city ORDER BY age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_age,
        LAST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY city ORDER BY age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit
    FROM cleaned
    WHERE segment = 'Consumer'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(running_min_age) OVER (PARTITION BY country ORDER BY age ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_running_min_age,
        AVG(running_min_age) OVER (PARTITION BY country ORDER BY age ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_running_min_age,
        COUNT(running_min_age) OVER (PARTITION BY country ORDER BY age ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_running_min_age,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY age ASC, id ASC) AS row_seq
    FROM enriched
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        email,
        city,
        signup_date,
        age,
        email_domain,
        resolved_segment,
        running_avg_age,
        running_min_age,
        running_count_age,
        last_known_credit_limit,
        last3_sum_running_min_age,
        last3_avg_running_min_age,
        last3_count_running_min_age,
        row_seq
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY credit_limit DESC NULLS LAST, id ASC) AS rn
        FROM metrics
    ) t
    WHERE rn = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        signup_date,
        email_domain,
        running_min_age,
        last_known_credit_limit,
        last3_sum_running_min_age,
        row_seq,
        ROUND(age / SUM(age) OVER (PARTITION BY city), 4) AS age_share,
        AVG(last3_avg_running_min_age) OVER (PARTITION BY city ORDER BY last3_sum_running_min_age ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS last3_avg_running_min_age_avg_3_prior
    FROM deduped
),
stats AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
        COUNT(*) AS num_rows,
        APPROX_PERCENTILE(last_known_credit_limit, ARRAY(0.25, 0.5, 0.75)) AS last_known_credit_limit_quartiles,
        COUNT(*) AS cnt
    FROM with_windows
    GROUP BY DATE_TRUNC('QUARTER', signup_date)
    HAVING COUNT(*) > 1
)
SELECT
    quarter_start,
    num_rows,
    last_known_credit_limit_quartiles,
    cnt
FROM stats
ORDER BY cnt DESC
