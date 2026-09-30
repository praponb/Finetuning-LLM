WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        ABS(premium - 2111.3) AS premium_distance
    FROM policies
    WHERE status = 'ACTIVE'
      AND start_date >= DATE '2025-01-24'
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM cleaned_policys s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claimed_amount <= 32441.2
        )
      AND s.premium_distance >= 1500.0
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        product,
        start_date,
        status,
        CASE WHEN status IN ('ACTIVE', 'CANCELLED') THEN 'group_a' WHEN status = 'EXPIRED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM filtered
    WHERE product IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status ORDER BY start_date, id), 3) AS start_date_percent_rank,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        FIRST_VALUE(start_date) OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        DATEDIFF(start_date, LAG(start_date) OVER w_status_by_start_date) AS days_since_prev_start_date,
        NTILE(10) OVER w_status_by_start_date AS start_date_decile,
        LEAD(start_date, 2) OVER w_status_by_start_date AS next2_start_date
    FROM derived
    WINDOW w_status_by_start_date AS (PARTITION BY status ORDER BY start_date, id)
)
SELECT
    id,
    product,
    start_date,
    status,
    start_date_percent_rank,
    second_start_date,
    first_start_date,
    days_since_prev_start_date,
    next2_start_date,
    ADD_MONTHS(first_start_date, 3) AS first_start_date_plus_3m,
    QUARTER(start_date) AS start_date_quarter
FROM ranked
