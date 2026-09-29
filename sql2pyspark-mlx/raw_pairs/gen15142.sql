WITH kept AS (
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.opened_date >= s.signup_date
        )
),
metrics AS (
    SELECT
        id,
        name,
        email,
        city,
        segment,
        is_active,
        age,
        CORR(age, credit_limit) OVER (PARTITION BY city, segment) corr_age_credit_limit,
        ROUND(age / SUM(age) OVER (PARTITION BY country), 4) age_share,
        CORR(credit_limit, age) OVER (PARTITION BY YEAR(signup_date)) corr_credit_limit_age
    FROM kept
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY corr_age_credit_limit, id) seq_no,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY age_share, id) event_seq,
        RANK() OVER (PARTITION BY city ORDER BY age_share) age_share_rank
    FROM metrics
),
ranked AS (
    SELECT
        id,
        name,
        city,
        segment,
        corr_age_credit_limit,
        age_share,
        corr_credit_limit_age,
        event_seq
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age_share_rank NULLS FIRST, id) AS rank_in_group
        FROM enriched
    ) x
    WHERE rank_in_group = 1
),
kept_keys AS (
    SELECT city
    FROM ranked
    EXCEPT DISTINCT
    SELECT city
    FROM customers
    WHERE age < 47
),
reconciled AS (
    SELECT s.*
    FROM ranked s
    JOIN kept_keys k ON s.city = k.city
),
eligible AS (
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.order_date >= '2025-04-01'
        )
),
wide AS (
    SELECT *
    FROM (
        SELECT
            city,
            segment,
            event_seq
        FROM eligible
    ) src
    PIVOT (
        MAX(event_seq) AS value_sum
        FOR segment IN ('Consumer' AS consumer, 'Corporate' AS corporate)
    )
)
SELECT city, CASE WHEN city RLIKE '^(Singapore|Berlin)$' THEN 'GROUP_A' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS city_zone
FROM wide;
