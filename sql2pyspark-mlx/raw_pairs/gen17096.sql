WITH cleaned AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        DAYOFWEEK(opened_date) AS opened_date_dayofweek,
        COALESCE(status, 'NONE') AS status_clean
    FROM accounts
    WHERE opened_date >= DATE '2023-09-12'
      AND account_type NOT IN ('LOAN', 'SAVINGS')
),
with_dims AS (
    SELECT
        e.id,
        e.customer_id,
        e.opened_date,
        e.opened_date_dayofweek,
        e.status_clean,
        d.signup_date
    FROM cleaned e
    LEFT JOIN customers d
        ON e.customer_id = d.id
    WHERE d.age < 58
),
scored AS (
    SELECT
        id,
        customer_id,
        opened_date,
        status_clean,
        signup_date,
        LAG(opened_date_dayofweek) OVER w_recent AS prev_opened_date_dayofweek,
        LEAD(opened_date_dayofweek) OVER w_recent AS next_opened_date_dayofweek,
        ROW_NUMBER() OVER w_recent AS row_seq,
        AVG(opened_date_dayofweek) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS opened_date_dayofweek_avg_2_prior
    FROM with_dims
    WINDOW w_recent AS (PARTITION BY customer_id ORDER BY opened_date, id)
),
joined AS (
    SELECT
        s.id,
        s.customer_id,
        s.signup_date,
        s.prev_opened_date_dayofweek,
        s.next_opened_date_dayofweek,
        s.row_seq,
        s.opened_date_dayofweek_avg_2_prior,
        c.city,
        c.signup_date AS customer_signup_date
    FROM scored s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
base AS (
    SELECT
        id,
        prev_opened_date_dayofweek,
        opened_date_dayofweek_avg_2_prior,
        customer_signup_date,
        DATEDIFF(customer_signup_date, signup_date) AS days_signup_date_to_customer_signup_date
    FROM joined
    WHERE city IS NOT DISTINCT FROM 'Chiang Mai'
),
kept AS (
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_type = 'FEE'
        )
)
SELECT
    CASE WHEN prev_opened_date_dayofweek >= 78 THEN 'HIGH' ELSE 'LOW' END AS prev_opened_date_dayofweek_tier,
    ROUND(AVG(days_signup_date_to_customer_signup_date), 2) AS avg_days_signup_date_to_customer_signup_date,
    AVG(opened_date_dayofweek_avg_2_prior) AS avg_opened_date_dayofweek_avg_2_prior,
    APPROX_PERCENTILE(days_signup_date_to_customer_signup_date, 0.95) AS days_signup_date_to_customer_signup_date_p95,
    APPROX_PERCENTILE(prev_opened_date_dayofweek, ARRAY(0.25, 0.5, 0.75)) AS prev_opened_date_dayofweek_quartiles,
    ROUND(AVG(prev_opened_date_dayofweek), 2) AS avg_prev_opened_date_dayofweek,
    GROUPING_ID() AS gid
FROM kept
GROUP BY ROLLUP (CASE WHEN prev_opened_date_dayofweek >= 78 THEN 'HIGH' ELSE 'LOW' END)
HAVING APPROX_PERCENTILE(days_signup_date_to_customer_signup_date, 0.95) > 0
ORDER BY avg_prev_opened_date_dayofweek DESC NULLS LAST;
