WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        country,
        segment,
        age,
        LAG(age, 1) OVER (PARTITION BY segment, country ORDER BY age, id) prev_age,
        SUM(age) OVER (PARTITION BY country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_age,
        AVG(credit_limit) OVER (ORDER BY credit_limit, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) credit_limit_avg_6_prior
    FROM customers
),
with_latest AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.account_opened_date,
        top1.balance
    FROM scored s
    JOIN LATERAL (
            SELECT c.opened_date AS account_opened_date, c.balance
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored_customers AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        country,
        prev_age,
        account_opened_date,
        balance,
        COUNT(credit_limit_avg_6_prior) OVER (PARTITION BY country ORDER BY credit_limit_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_credit_limit_avg_6_prior,
        AVG(credit_limit_avg_6_prior) OVER (PARTITION BY country ORDER BY credit_limit_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_credit_limit_avg_6_prior,
        SUM(credit_limit_avg_6_prior) OVER (PARTITION BY country ORDER BY credit_limit_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_credit_limit_avg_6_prior,
        LEAD(cumulative_age, 2) OVER (PARTITION BY segment ORDER BY account_opened_date, id) next_cumulative_age
    FROM with_latest
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(next_cumulative_age) OVER (PARTITION BY country ORDER BY running_sum_credit_limit_avg_6_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_min_next_cumulative_age,
        SUM(next_cumulative_age) OVER (PARTITION BY country ORDER BY running_sum_credit_limit_avg_6_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_sum_next_cumulative_age,
        MAX(next_cumulative_age) OVER (PARTITION BY country ORDER BY running_sum_credit_limit_avg_6_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_max_next_cumulative_age,
        AVG(next_cumulative_age) OVER (PARTITION BY country ORDER BY running_sum_credit_limit_avg_6_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_avg_next_cumulative_age
    FROM scored_customers
),
latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.prev_age,
        s.account_opened_date,
        s.running_count_credit_limit_avg_6_prior,
        s.running_sum_credit_limit_avg_6_prior,
        s.next_cumulative_age,
        s.last4_min_next_cumulative_age,
        x.started_at,
        x.device
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.started_at, c.device
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.account_opened_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Running and moving aggregates
SELECT
    id,
    account_opened_date,
    running_count_credit_limit_avg_6_prior,
    running_sum_credit_limit_avg_6_prior,
    next_cumulative_age,
    last4_min_next_cumulative_age,
    started_at,
    ROUND(running_sum_credit_limit_avg_6_prior / SUM(running_sum_credit_limit_avg_6_prior) OVER (PARTITION BY device), 4) running_sum_credit_limit_avg_6_prior_share,
    ROW_NUMBER() OVER (PARTITION BY device ORDER BY prev_age, id) seq_no,
    SUM(next_cumulative_age) OVER (PARTITION BY MONTH(started_at), device ORDER BY next_cumulative_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_next_cumulative_age
FROM latest_match;
