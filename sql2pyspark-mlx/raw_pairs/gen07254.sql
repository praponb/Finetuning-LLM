WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY MONTH(signup_date) ORDER BY age DESC) age_percent_rank,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY city, segment), 4) credit_limit_share,
        CORR(age, credit_limit) OVER (PARTITION BY country, city) corr_age_credit_limit
    FROM customers
    WHERE signup_date <= '2024-09-01'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY age_percent_rank, id) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        city,
        is_active,
        age,
        credit_limit_share,
        AVG(credit_limit) OVER (ORDER BY signup_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) credit_limit_avg_2_prior,
        CUME_DIST() OVER (PARTITION BY segment, country ORDER BY credit_limit_share DESC) credit_limit_share_cume_dist,
        FIRST_VALUE(age_percent_rank) IGNORE NULLS OVER (PARTITION BY segment ORDER BY credit_limit_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_age_percent_rank,
        SUM(corr_age_credit_limit) OVER w_running running_corr_age_credit_limit
    FROM latest
    WINDOW w_running AS (PARTITION BY DATE_TRUNC('month', signup_date), country ORDER BY credit_limit_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.monthly_fee,
        top1.end_date
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.monthly_fee, c.end_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.start_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last_customers AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.city,
        s.is_active,
        s.age,
        s.credit_limit_share,
        top1.status
    FROM with_last s
    JOIN LATERAL (
            SELECT c.status
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_customers AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit_share DESC NULLS FIRST, id) AS row_num
        FROM with_last_customers
    ) ranked
    WHERE row_num = 1
),
with_last_customers_2 AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.subscription_status
    FROM latest_customers s
    JOIN LATERAL (
            SELECT c.status AS subscription_status
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Ranking and gap features
SELECT
    *,
    PERCENT_RANK() OVER (PARTITION BY country ORDER BY credit_limit_share DESC NULLS LAST) credit_limit_share_percent_rank,
    ROUND(age / SUM(age) OVER (PARTITION BY city), 4) age_share
FROM with_last_customers_2;
