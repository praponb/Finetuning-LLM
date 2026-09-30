WITH cleaned AS (
    SELECT
        *,
        REPLACE(name, ' ', '-') AS name_slug,
        credit_limit + age AS credit_limit_plus_age
    FROM customers
    WHERE LENGTH(name) > 6
),
enriched AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY country ORDER BY credit_limit NULLS LAST) AS credit_limit_cume_dist,
        NTILE(4) OVER (PARTITION BY city ORDER BY credit_limit, id) AS credit_limit_bucket,
        ROW_NUMBER() OVER (ORDER BY credit_limit, id) AS row_seq,
        LEAD(credit_limit_plus_age, 1) OVER (ORDER BY credit_limit_plus_age, id) AS next_credit_limit_plus_age
    FROM cleaned
),
with_windows AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        name_slug,
        credit_limit_plus_age,
        credit_limit_cume_dist,
        credit_limit_bucket,
        row_seq,
        next_credit_limit_plus_age,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY city), 4) AS row_seq_share,
        LAST_VALUE(credit_limit_cume_dist, TRUE) OVER (PARTITION BY country ORDER BY credit_limit_plus_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit_cume_dist
    FROM enriched
),
latest AS (
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY signup_date) AS rank_in_group
    FROM with_windows
    QUALIFY rank_in_group = 1
),
valid AS (
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        name_slug,
        credit_limit_plus_age,
        credit_limit_cume_dist,
        credit_limit_bucket,
        row_seq,
        next_credit_limit_plus_age,
        rank_in_group,
        CASE WHEN credit_limit_plus_age >= 85 THEN 'High' WHEN credit_limit_plus_age >= 14 THEN 'Medium' ELSE 'Low' END AS credit_limit_plus_age_level
    FROM latest
    WHERE city IS NOT NULL
),
deduped AS (
    SELECT *
    FROM valid
    QUALIFY RANK() OVER (PARTITION BY country ORDER BY rank_in_group DESC) = 1
),
remaining_keys AS (
    SELECT id, country
    FROM deduped
    INTERSECT
    SELECT id, country
    FROM customers
    WHERE LENGTH(email) > 12
),
kept_rows AS (
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.country = k.country
),
daily AS (
    SELECT
        DATE_TRUNC('WEEK', signup_date) AS week_start,
        DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
        DATE_TRUNC('MONTH', signup_date) AS activity_month,
        BIT_OR(age) AS age_bit_or,
        SIZE(COLLECT_SET(segment)) AS num_segments,
        ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN country = 'US' THEN 1 END), 0), 2) AS credit_limit_per_us,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        APPROX_PERCENTILE(age, 0.95) AS age_p95
    FROM kept_rows
    GROUP BY DATE_TRUNC('WEEK', signup_date), DATE_TRUNC('QUARTER', signup_date), DATE_TRUNC('MONTH', signup_date)
    HAVING ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN country = 'US' THEN 1 END), 0), 2) >= 0
)
SELECT
    week_start,
    quarter_start,
    activity_month,
    age_bit_or,
    num_segments,
    city_list,
    age_p95
FROM daily
