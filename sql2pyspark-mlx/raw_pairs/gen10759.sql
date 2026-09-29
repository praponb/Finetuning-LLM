WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(promo_code, ' - ', channel) AS promo_code_channel,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM promotions
    WHERE channel IS NOT NULL
      AND channel NOT IN ('store', 'partner')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('week', ends_on) AS ends_on_period,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM filtered
    WHERE channel IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(ends_on_period) OVER (PARTITION BY channel ORDER BY ends_on_period, id) IS NULL
                 OR ends_on_period > LAG(ends_on_period) OVER (PARTITION BY channel ORDER BY ends_on_period, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY channel ORDER BY ends_on_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM sessionized a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'store') = pr.channel
       AND a.ends_on_period BETWEEN pr.starts_on AND pr.ends_on
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN ends_on_period - LAG(ends_on_period) OVER (PARTITION BY channel ORDER BY ends_on_period, id, promo_id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM attributed
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY channel ORDER BY ends_on_period, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps_2
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN ends_on_period - LAG(ends_on_period) OVER (PARTITION BY channel ORDER BY ends_on_period, id, promo_id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM islands
),
sessionized_promotions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY channel ORDER BY ends_on_period, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
-- Running and moving aggregates
SELECT
    id,
    channel,
    starts_on,
    ends_on,
    promo_code_channel,
    channel_filled,
    ends_on_period,
    resolved_channel,
    is_new_burst,
    burst_id,
    promo_id,
    promotion_discount_pct,
    new_group_flag,
    session_no,
    COUNT(session_start_flag) OVER w_hist AS last7_count_session_start_flag,
    MAX(session_start_flag) OVER w_hist AS last7_max_session_start_flag,
    SUM(session_start_flag) OVER w_hist AS last7_sum_session_start_flag,
    AVG(is_new_burst) OVER (PARTITION BY channel, promo_code ORDER BY session_start_flag, id, promo_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS is_new_burst_avg_2
FROM sessionized_promotions
WINDOW w_hist AS (PARTITION BY channel ORDER BY ends_on_period, id, promo_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
