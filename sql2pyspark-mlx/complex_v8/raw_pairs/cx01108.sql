WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        CASE WHEN product IN ('HOME', 'AUTO') THEN 'group_a' WHEN product = 'LIFE' THEN 'group_b' ELSE 'other' END AS product_group,
        CASE WHEN status IN ('LAPSED', 'CANCELLED') THEN 'group_a' WHEN status = 'EXPIRED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM policies
    WHERE status = 'LAPSED'
      AND end_date < DATE '2026-07-11'
),
latest AS (
    -- Keep the latest row per status
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        product_group,
        status_group
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date DESC, id DESC) AS recency_rank
        FROM valid_policys
    ) ranked
    WHERE recency_rank = 1
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        holder_id,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        COUNT(*) AS row_count,
        ROUND(100 * COUNT(CASE WHEN status = 'LAPSED' THEN 1 END) / COUNT(*), 1) AS pct_status_lapsed,
        COUNT(DISTINCT id) AS distinct_ids
    FROM latest
    WHERE status_group IN ('other', 'group_b')
    GROUP BY status, holder_id, DATE_FORMAT(end_date, 'yyyy-MM')
),
detail_rows_policys AS (
    -- One row per policy
    SELECT
        s.status,
        s.holder_id,
        s.end_date_ym,
        s.row_count,
        s.distinct_ids,
        p.id AS policy_id,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date
    FROM per_group s
    JOIN policies p ON p.holder_id = s.holder_id
)
SELECT status, holder_id, metric, value
FROM (
    SELECT status, holder_id, row_count, distinct_ids
    FROM detail_rows_policys
) src
UNPIVOT (
    value FOR metric IN (row_count, distinct_ids)
) unp
ORDER BY status, holder_id, metric
