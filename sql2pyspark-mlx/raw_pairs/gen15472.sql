WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(start_date) AS start_date_month_end
    FROM subscriptions
    WHERE end_date >= DATE '2023-07-06'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 4) AS monthly_fee_share,
        LEAD(monthly_fee, 1, 0) OVER (PARTITION BY plan ORDER BY start_date, id) AS next_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_5_prior
    FROM cleaned
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY status ORDER BY monthly_fee_avg_5_prior DESC) AS monthly_fee_avg_5_prior_rank,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share_2,
        LAST_VALUE(monthly_fee_share, TRUE) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee_share,
        FIRST_VALUE(next_monthly_fee) IGNORE NULLS OVER (PARTITION BY status, customer_id ORDER BY monthly_fee_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_monthly_fee,
        NTILE(10) OVER (PARTITION BY customer_id ORDER BY next_monthly_fee DESC, id) AS next_monthly_fee_bucket
    FROM enriched
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, status
    FROM scored
    INTERSECT ALL
    SELECT id, status
    FROM subscriptions
    WHERE MONTH(start_date) IN (9, 10, 11)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.id = k.id AND s.status = k.status
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label
    FROM verified
    WHERE monthly_fee < 170
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        monthly_fee_share,
        next_monthly_fee,
        monthly_fee_avg_5_prior_rank,
        monthly_fee_share_2,
        last_known_monthly_fee_share,
        next_monthly_fee_bucket,
        end_date_label,
        CEIL(first_known_next_monthly_fee) AS first_known_next_monthly_fee_whole
    FROM valid
    WHERE monthly_fee_share_2 BETWEEN 0.36 AND 0.44
)
SELECT
    id,
    status,
    next_monthly_fee,
    monthly_fee_share_2,
    last_known_monthly_fee_share,
    first_known_next_monthly_fee_whole,
    CASE WHEN status RLIKE '^(EXPIRED|PAUSED)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS status_zone
FROM filtered
ORDER BY status NULLS LAST
