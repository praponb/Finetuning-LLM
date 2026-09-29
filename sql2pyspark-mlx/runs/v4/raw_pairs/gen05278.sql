WITH cleaned AS (
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        status,
        CASE WHEN plan = 'standard' THEN 1 ELSE 0 END AS is_standard
    FROM subscriptions
    WHERE start_date > DATE '2023-12-16'
),
with_windows AS (
    SELECT
        *,
        SUM(is_standard) OVER (ORDER BY is_standard ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_standard,
        SUM(monthly_fee) OVER (PARTITION BY status ORDER BY end_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee
    FROM cleaned
),
scored AS (
    SELECT
        *,
        AVG(cumulative_is_standard) OVER (PARTITION BY status ORDER BY end_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS cumulative_is_standard_avg_6_prior,
        LEAD(monthly_fee, 2, 0) OVER (PARTITION BY status ORDER BY running_monthly_fee ASC, id ASC) AS next_monthly_fee,
        LAG(monthly_fee, 1, 0) OVER (ORDER BY running_monthly_fee ASC, id ASC) AS prev_monthly_fee,
        LEAD(running_monthly_fee, 1) OVER (PARTITION BY plan ORDER BY is_standard ASC, id ASC) AS next_running_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date DESC, id ASC) AS event_seq
    FROM with_windows
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee ASC NULLS FIRST, id ASC) AS dedup_rank
        FROM scored
    ) t
    WHERE dedup_rank = 1
),
kept_keys AS (
    SELECT customer_id, id
    FROM deduped
    EXCEPT DISTINCT
    SELECT customer_id, id
    FROM subscriptions
    WHERE customer_id IN (11, 13, 16, 37)
),
kept_rows AS (
    SELECT s.*
    FROM deduped s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
with_windows_subscriptions AS (
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        status,
        is_standard,
        cumulative_is_standard,
        running_monthly_fee,
        cumulative_is_standard_avg_6_prior,
        next_monthly_fee,
        prev_monthly_fee,
        next_running_monthly_fee,
        event_seq,
        dedup_rank,
        RANK() OVER (PARTITION BY plan ORDER BY cumulative_is_standard DESC NULLS LAST) AS cumulative_is_standard_rank,
        CUME_DIST() OVER (ORDER BY next_running_monthly_fee ASC NULLS FIRST) AS next_running_monthly_fee_cume_dist
    FROM kept_rows
)
SELECT *
FROM (
    SELECT
        plan,
        next_monthly_fee,
        prev_monthly_fee
    FROM with_windows_subscriptions
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (next_monthly_fee AS NEXT_MONTHLY_FEE_VAL, prev_monthly_fee AS PREV_MONTHLY_FEE_VAL)
)
