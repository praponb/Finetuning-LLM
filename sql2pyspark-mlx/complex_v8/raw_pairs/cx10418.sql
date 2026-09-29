WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 1.5, 2) AS premium_adj,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE status <> 'EXPIRED'
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        start_date,
        status,
        premium_bucket,
        premium_filled,
        ROUND(premium_filled * 0.25, 2) AS premium_filled_adj,
        DAYOFWEEK(end_date) AS end_date_dayofweek
    FROM base_policys
)
-- Subtotals with GROUPING SETS
SELECT
    holder_id,
    status,
    end_date_dayofweek,
    GROUPING(holder_id) AS is_total_holder_id,
    MAX(end_date_dayofweek) AS max_end_date_dayofweek,
    ROUND(100 * COUNT(CASE WHEN status = 'EXPIRED' THEN 1 END) / COUNT(*), 1) AS pct_status_expired
FROM tagged
GROUP BY GROUPING SETS ((holder_id, status, end_date_dayofweek), (holder_id), (status), ())
