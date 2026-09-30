WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        COALESCE(monthly_fee, 0) monthly_fee_filled
    FROM subscriptions
    WHERE monthly_fee <= 110
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        monthly_fee,
        monthly_fee_filled,
        LAG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY monthly_fee ASC, id ASC) prev_monthly_fee,
        LEAD(monthly_fee) OVER (PARTITION BY customer_id ORDER BY monthly_fee ASC, id ASC) next_monthly_fee,
        LAG(monthly_fee_filled) OVER (PARTITION BY customer_id ORDER BY monthly_fee ASC, id ASC) prev_monthly_fee_filled,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee ASC, id ASC) seq_no,
        SUM(monthly_fee) OVER (PARTITION BY DATE_TRUNC('WEEK', start_date), status ORDER BY monthly_fee_filled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_monthly_fee
    FROM filtered
    WHERE start_date > '2024-01-15'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(prev_monthly_fee) IGNORE NULLS OVER (ORDER BY seq_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_prev_monthly_fee,
        AVG(running_monthly_fee) OVER (ORDER BY monthly_fee ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) running_monthly_fee_avg_2_prior,
        ROW_NUMBER() OVER (ORDER BY prev_monthly_fee ASC, id ASC) seq_no_2
    FROM enriched
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY seq_no DESC, id ASC) row_seq,
        SUM(first_known_prev_monthly_fee) OVER (ORDER BY prev_monthly_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_first_known_prev_monthly_fee,
        NTILE(10) OVER (ORDER BY first_known_prev_monthly_fee DESC, id ASC) first_known_prev_monthly_fee_bucket
    FROM metrics
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_windows
    EXCEPT
    SELECT id
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    seq_no,
    CASE WHEN prev_monthly_fee_filled >= 88 THEN 'HIGH' ELSE 'LOW' END AS prev_monthly_fee_filled_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    PERCENTILE_APPROX(prev_monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS prev_monthly_fee_quartiles
FROM verified
GROUP BY seq_no, CASE WHEN prev_monthly_fee_filled >= 88 THEN 'HIGH' ELSE 'LOW' END;
