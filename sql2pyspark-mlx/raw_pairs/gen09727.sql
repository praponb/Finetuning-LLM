WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, age + credit_limit age_plus_credit_limit
    FROM customers
    WHERE signup_date > DATE '2023-06-15'
      AND city IS NOT NULL
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.country,
        s.city,
        s.signup_date,
        s.is_active,
        s.credit_limit,
        s.age,
        latest.start_date
    FROM filtered s
    JOIN LATERAL (
            SELECT c.start_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.end_date <= s.signup_date
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.balance > 46000
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CORR(credit_limit, age) OVER (PARTITION BY city, country) corr_credit_limit_age,
        RANK() OVER (ORDER BY credit_limit DESC NULLS LAST) credit_limit_rank,
        CORR(age, credit_limit) OVER (PARTITION BY country) corr_age_credit_limit,
        AVG(credit_limit) OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) credit_limit_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY YEAR(signup_date), country ORDER BY start_date, id) event_seq
    FROM kept
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        signup_date,
        credit_limit,
        age,
        corr_credit_limit_age,
        credit_limit_rank,
        corr_age_credit_limit,
        credit_limit_avg_5_prior,
        event_seq,
        LEAD(event_seq, 1) OVER (PARTITION BY city, country ORDER BY corr_credit_limit_age, id) next_event_seq,
        LEAD(event_seq, 1) OVER (PARTITION BY city ORDER BY signup_date, id) next_event_seq_2,
        ROUND(credit_limit_rank / SUM(credit_limit_rank) OVER (PARTITION BY YEAR(signup_date), country), 4) credit_limit_rank_share,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', start_date), country ORDER BY start_date, id) seq_no
    FROM enriched
)
SELECT
    id,
    country,
    signup_date,
    credit_limit,
    age,
    corr_credit_limit_age,
    corr_age_credit_limit,
    credit_limit_avg_5_prior,
    event_seq
FROM metrics;
