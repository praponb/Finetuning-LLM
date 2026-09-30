WITH scoped AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        signup_date
    FROM listeners
    WHERE signup_date >= DATE '2024-08-02'
      AND plan = 'family'
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
              AND p.ms_played < 302279
        )
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
              AND p.ms_played > 160264
        )
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(signup_date) IGNORE NULLS OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_signup_date,
        LAG(signup_date) OVER (PARTITION BY MONTH(signup_date), plan ORDER BY signup_date DESC, id) AS prev_signup_date,
        NTH_VALUE(signup_date, 2) OVER (PARTITION BY MONTH(signup_date), plan ORDER BY signup_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_signup_date,
        LEAD(signup_date, 2) OVER (ORDER BY signup_date, id) AS next2_signup_date
    FROM with_activity
)
SELECT
    id,
    signup_date,
    prev_signup_date,
    second_signup_date,
    next2_signup_date
FROM with_windows
ORDER BY signup_date DESC, id
LIMIT 10
