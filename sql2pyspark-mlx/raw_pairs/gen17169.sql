WITH eligible AS (
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at < s.signup_date
        )
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date DESC, id) AS row_num
        FROM eligible
    ) t
    WHERE row_num = 1
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY row_num DESC, id) AS seq_no,
        SUM(credit_limit) OVER (PARTITION BY country, segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_credit_limit,
        LAST_VALUE(segment, TRUE) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_segment,
        SUM(credit_limit) OVER (ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_credit_limit_2,
        MIN(age) OVER (PARTITION BY segment) AS segment_min_age
    FROM latest
),
latest_customers AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY signup_date DESC) AS latest_rank
        FROM scored
    ) ranked
    WHERE latest_rank = 1
),
metrics AS (
    SELECT
        id,
        name,
        email,
        city,
        signup_date,
        is_active,
        age,
        row_num,
        seq_no,
        cumulative_credit_limit,
        latest_rank,
        SUM(segment_min_age) OVER (PARTITION BY segment ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_segment_min_age,
        ROW_NUMBER() OVER w_part AS event_seq,
        SUM(seq_no) OVER (PARTITION BY segment, country ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_seq_no
    FROM latest_customers
    WINDOW w_part AS (PARTITION BY country ORDER BY cumulative_credit_limit, id)
),
summary AS (
    SELECT
        seq_no,
        DATE_TRUNC('YEAR', signup_date) AS year_start,
        AVG(seq_no) FILTER (WHERE city = 'Singapore') AS singapore_avg_seq_no,
        MIN(running_segment_min_age) AS min_running_segment_min_age
    FROM metrics
    GROUP BY seq_no, DATE_TRUNC('YEAR', signup_date)
)
SELECT
    seq_no,
    year_start,
    singapore_avg_seq_no
FROM summary
ORDER BY seq_no DESC
