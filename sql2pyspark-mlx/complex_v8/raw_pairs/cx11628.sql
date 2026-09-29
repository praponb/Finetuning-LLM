WITH valid_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        DATE_FORMAT(end_date, 'MMM yyyy') AS end_date_label,
        MONTH(start_date) AS start_date_month
    FROM policies
    WHERE end_date >= DATE '2026-01-24'
      AND start_date < DATE '2025-03-06'
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        LOWER(TRIM(end_date_label)) AS end_date_label_clean,
        LENGTH(TRIM(status)) AS status_len
    FROM valid_policys
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(status_len, 1, 0) OVER w_start_date_month_holder_id_b AS prev_status_len,
        COUNT(*) OVER (PARTITION BY status_len) AS rows_in_status_len
    FROM tagged
    WINDOW w_start_date_month_holder_id_b AS (PARTITION BY start_date_month, holder_id ORDER BY premium DESC, id)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        SUM(rows_in_status_len) FILTER (WHERE status = 'EXPIRED') AS rows_in_status_len_status_expired,
        COUNT(*) AS cnt,
        SUM(premium) AS total_premium,
        PERCENTILE_APPROX(rows_in_status_len, 0.5) AS median_rows_in_status_len,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN status NOT IN ('ACTIVE', 'EXPIRED') THEN 1 ELSE 0 END) AS n_status_not_in_active_expired
    FROM windowed
    GROUP BY DATE_TRUNC('MONTH', end_date)
),
labeled AS (
    -- Derive helper columns
    SELECT
        end_date_month,
        rows_in_status_len_status_expired,
        cnt,
        LEAST(cnt, distinct_ids) AS least_cnt_distinct_ids,
        DATE_TRUNC('WEEK', end_date_month) AS end_date_month_week
    FROM rollup_base
)
SELECT end_date_month, measure, reading
FROM (
    SELECT end_date_month, cnt, least_cnt_distinct_ids
    FROM labeled
) src
UNPIVOT (
    reading FOR measure IN (cnt, least_cnt_distinct_ids)
) unp
