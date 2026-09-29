WITH valid AS (
    SELECT *, ROUND(DATEDIFF(end_date, start_date) / 30, 1) AS months_start_date_to_end_date
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND status IN ('PAUSED', 'ACTIVE', 'CANCELLED')
),
scored AS (
    SELECT
        *,
        LEAD(months_start_date_to_end_date, 1) OVER w_part AS next_months_start_date_to_end_date,
        NTILE(5) OVER (ORDER BY monthly_fee ASC, id ASC) AS monthly_fee_bucket,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 4) AS monthly_fee_share
    FROM valid
    WHERE status = 'ACTIVE'
    WINDOW w_part AS (ORDER BY end_date ASC, id ASC)
),
enriched AS (
    SELECT
        s.*,
        d.credit_limit,
        d.age
    FROM scored s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
remaining_keys AS (
    SELECT customer_id, plan
    FROM enriched
    EXCEPT ALL
    SELECT customer_id, plan
    FROM subscriptions
    WHERE end_date > '2024-01-31'
),
kept_rows AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.plan = k.plan
),
with_dims AS (
    SELECT
        src.id,
        src.customer_id,
        src.start_date,
        src.end_date,
        src.next_months_start_date_to_end_date,
        src.monthly_fee_share,
        src.age,
        p.age AS customer_age,
        p.credit_limit AS customer_credit_limit
    FROM kept_rows src
    INNER JOIN customers p
        ON src.customer_id = p.id
),
enriched_subscriptions AS (
    SELECT
        id,
        start_date,
        next_months_start_date_to_end_date,
        monthly_fee_share,
        STDDEV_SAMP(next_months_start_date_to_end_date) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_next_months_start_date_to_end_date
    FROM with_dims
),
metrics AS (
    SELECT
        start_date,
        monthly_fee_share,
        customer_id_stddev_samp_next_months_start_date_to_end_date,
        CUME_DIST() OVER (ORDER BY customer_id_stddev_samp_next_months_start_date_to_end_date ASC NULLS LAST) AS customer_id_stddev_samp_next_months_start_date_to_end_date_cume_dist
    FROM enriched_subscriptions
)
SELECT
    start_date,
    monthly_fee_share,
    customer_id_stddev_samp_next_months_start_date_to_end_date,
    customer_id_stddev_samp_next_months_start_date_to_end_date_cume_dist
FROM metrics
