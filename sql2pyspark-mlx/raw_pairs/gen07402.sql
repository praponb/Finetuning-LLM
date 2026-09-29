WITH enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        b.email,
        b.credit_limit
    FROM subscriptions a
    JOIN customers b
        ON a.customer_id <=> b.id
    WHERE b.name IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        start_date,
        status,
        email,
        AVG(monthly_fee) OVER (PARTITION BY YEAR(end_date), customer_id ORDER BY end_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_3_prior,
        CUME_DIST() OVER (ORDER BY credit_limit NULLS FIRST) AS credit_limit_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee, id) AS seq_no,
        MAX(monthly_fee) OVER (PARTITION BY YEAR(start_date), customer_id) AS start_date_year_max_monthly_fee,
        NTILE(10) OVER (ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket
    FROM enriched
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT_WS('/', status, email) AS status_email_key
    FROM with_windows
    WHERE start_date > TO_DATE('2023-05-04')
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status
    FROM cleaned
    EXCEPT
    SELECT status
    FROM subscriptions
    WHERE end_date < '2023-04-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN remaining_keys k ON s.status = k.status
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        start_date,
        email,
        monthly_fee_avg_3_prior,
        RANK() OVER w_grp AS seq_no_rank,
        SUM(credit_limit_cume_dist) OVER (PARTITION BY status) AS status_sum_credit_limit_cume_dist,
        ROUND(start_date_year_max_monthly_fee / SUM(start_date_year_max_monthly_fee) OVER (PARTITION BY status), 4) AS start_date_year_max_monthly_fee_share
    FROM reconciled
    WINDOW w_grp AS (ORDER BY seq_no DESC NULLS LAST)
)
SELECT
    start_date,
    email,
    seq_no_rank,
    status_sum_credit_limit_cume_dist,
    start_date_year_max_monthly_fee_share
FROM scored
ORDER BY start_date NULLS FIRST, start_date_year_max_monthly_fee_share
