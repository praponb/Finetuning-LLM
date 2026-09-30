WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LAST_DAY(signup_date) AS signup_date_month_end
    FROM customers
    WHERE is_active
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date_month_end DESC NULLS LAST, id) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.status,
        top1.amount
    FROM ranked s
    JOIN LATERAL (
            SELECT c.status, c.amount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        city,
        signup_date,
        credit_limit,
        age,
        latest_rank,
        status,
        amount,
        SUM(credit_limit) OVER w_part AS running_sum_credit_limit,
        AVG(credit_limit) OVER w_part AS running_avg_credit_limit,
        MIN(credit_limit) OVER w_part AS running_min_credit_limit,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY city, segment), 4) AS credit_limit_share
    FROM with_latest
    WHERE city <> 'Bangkok'
    WINDOW w_part AS (PARTITION BY city ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(running_sum_credit_limit) OVER (ORDER BY credit_limit_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_sum_credit_limit,
        SUM(age) OVER (PARTITION BY MONTH(signup_date), city ORDER BY credit_limit_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_age,
        LAST_VALUE(credit_limit_share, TRUE) OVER (PARTITION BY city, status ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit_share,
        CORR(credit_limit, age) OVER (PARTITION BY status, city) AS corr_credit_limit_age,
        AVG(age) OVER (ORDER BY credit_limit, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS age_avg_6_prior
    FROM with_windows
)
SELECT
    id,
    email,
    city,
    credit_limit,
    latest_rank,
    status,
    running_sum_credit_limit,
    running_avg_credit_limit,
    credit_limit_share,
    CASE WHEN city RLIKE '^(Chiang Mai|Tokyo)$' THEN 'PRIORITY' WHEN city RLIKE '^(Bangkok|New York)$' THEN 'CORE' ELSE 'GROUP_B' END AS city_zone
FROM scored
ORDER BY id DESC
