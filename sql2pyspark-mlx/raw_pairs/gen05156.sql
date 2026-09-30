WITH scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit DESC, id) row_seq,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('month', signup_date), country ORDER BY credit_limit, id) credit_limit_bucket,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY credit_limit) credit_limit_percent_rank
    FROM customers
    WHERE country NOT IN ('TH', 'DE', 'UK')
),
base AS (
    SELECT
        id,
        name,
        email,
        segment,
        signup_date,
        age,
        row_seq,
        credit_limit_bucket,
        credit_limit_percent_rank,
        credit_limit_bucket * credit_limit_percent_rank credit_limit_bucket_times_credit_limit_percent_rank,
        CONCAT(city, ' - ', country) city_country,
        COALESCE(segment, 'UNKNOWN') segment_filled
    FROM scored
    WHERE segment IS NOT NULL
),
base_customers AS (
    SELECT
        *,
        UPPER(name) name_upper,
        LEAST(age, credit_limit_bucket) least_age_credit_limit_bucket
    FROM base
    WHERE signup_date > DATE '2023-04-19'
      AND credit_limit_bucket_times_credit_limit_percent_rank < 71
),
metrics AS (
    SELECT
        *,
        RANK() OVER (ORDER BY credit_limit_bucket DESC NULLS LAST) credit_limit_bucket_rank,
        LAG(row_seq, 1) OVER (PARTITION BY segment ORDER BY credit_limit_percent_rank, id) prev_row_seq,
        ROUND(age / SUM(age) OVER (PARTITION BY segment), 4) age_share
    FROM base_customers
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit_percent_rank NULLS LAST, id) AS dedup_rank
        FROM metrics
    ) t
    WHERE dedup_rank = 1
)
SELECT
    CASE WHEN prev_row_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_row_seq_tier,
    segment,
    SUM(CASE WHEN segment = 'Consumer' THEN least_age_credit_limit_bucket ELSE 0 END) AS consumer_least_age_credit_limit_bucket,
    PERCENTILE_APPROX(credit_limit_bucket_rank, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_bucket_rank_quartiles,
    COUNT_IF(segment = 'Consumer') AS consumer_events,
    SIZE(COLLECT_SET(segment)) AS num_segments
FROM ranked
GROUP BY GROUPING SETS ((CASE WHEN prev_row_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END, segment), (CASE WHEN prev_row_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
HAVING num_segments > 1
