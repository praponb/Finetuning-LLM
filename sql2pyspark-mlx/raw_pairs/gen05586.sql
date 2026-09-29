WITH with_windows AS (
    SELECT
        *,
        LEAD(age) OVER w_running next_age,
        LAG(age) OVER w_running prev_age,
        LAG(credit_limit) OVER w_running prev_credit_limit,
        LEAD(credit_limit) OVER w_running next_credit_limit,
        ROW_NUMBER() OVER w_running event_seq
    FROM customers
    WHERE segment <> 'Corporate'
    WINDOW w_running AS (PARTITION BY country ORDER BY signup_date, id)
)
SELECT
    id,
    is_active,
    next_age,
    next_credit_limit,
    event_seq
FROM with_windows
ORDER BY id DESC NULLS FIRST, event_seq;
