WITH recent AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE status NOT IN ('ACTIVE', 'EXPIRED')
      AND YEAR(end_date) = 2026
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, MAX(premium) AS max_policy_premium
    FROM policies
    GROUP BY holder_id
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.max_policy_premium
    FROM recent s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(premium) OVER (PARTITION BY YEAR(start_date), status ORDER BY end_date DESC NULLS FIRST, id) AS prev_premium,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(start_date), status ORDER BY end_date DESC NULLS FIRST), 3) AS end_date_cume_dist,
        SUM(premium) OVER (ORDER BY max_policy_premium DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS premium_sum_4,
        ROUND(max_policy_premium / NULLIF(SUM(max_policy_premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), status), 0), 4) AS max_policy_premium_share_of_start_date_month_sta,
        MIN(max_policy_premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), status) AS start_date_month_status_min_max_policy_premium,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), status) AS rows_in_start_date_month_status
    FROM with_counts
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        YEAR(start_date) AS start_date_year
    FROM with_windows
)
SELECT
    id,
    holder_id,
    product,
    premium,
    status,
    prev_premium,
    end_date_cume_dist,
    premium_sum_4,
    max_policy_premium_share_of_start_date_month_sta,
    rows_in_start_date_month_status,
    start_date_year,
    CASE WHEN premium_sum_4 >= 11300.0 THEN 'A' WHEN premium_sum_4 >= 6120.0 THEN 'B' ELSE 'C' END AS premium_sum_4_tier
FROM tagged
ORDER BY holder_id DESC, end_date_cume_dist
