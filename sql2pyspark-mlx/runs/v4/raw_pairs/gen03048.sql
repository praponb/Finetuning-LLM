WITH filtered AS (
    SELECT
        *,
        INITCAP(TRIM(country)) AS country_title,
        DATE_FORMAT(signup_date, 'yyyy-MM-dd') AS signup_date_label,
        COALESCE(segment, 'NONE') AS segment_filled
    FROM customers
    WHERE signup_date >= DATE '2024-02-13'
      AND segment IS NOT NULL
),
enriched AS (
    SELECT
        *,
        CORR(credit_limit, age) OVER (PARTITION BY country) AS corr_credit_limit_age,
        ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY credit_limit DESC, id) AS row_seq
    FROM filtered
),
with_last AS (
    SELECT
        s.*,
        x.order_channel,
        x.discount
    FROM enriched s
    JOIN LATERAL (
            SELECT c.channel AS order_channel, c.discount
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date <= s.signup_date
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
filtered_customers AS (
    SELECT *, LENGTH(email) AS email_length
    FROM with_last
    WHERE age = 63
)
SELECT *
FROM (
    SELECT
        order_channel,
        country,
        row_seq
    FROM filtered_customers
) src
PIVOT (
    MAX(row_seq) AS value_sum
    FOR country IN ('SG', 'TH')
);
