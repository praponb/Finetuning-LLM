WITH cleaned_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN product IN ('HEALTH', 'HOME') THEN 'group_a' WHEN product = 'TRAVEL' THEN 'group_b' ELSE 'other' END AS product_group,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE premium >= 2111.3
      AND end_date >= DATE '2025-11-20'
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM cleaned_policys s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.status = 'IN_REVIEW'
        )
      AND s.holder_id IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(days_start_date_to_end_date) OVER (PARTITION BY holder_id, product_group) AS holder_id_product_group_sum_days_start_date_to_e,
        LAST_VALUE(days_start_date_to_end_date) IGNORE NULLS OVER (PARTITION BY status ORDER BY days_start_date_to_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_start_date_to_end_date,
        ROUND(CUME_DIST() OVER w_end_date_year_by_start_date, 3) AS start_date_cume_dist,
        LEAD(start_date, 2) OVER (PARTITION BY status ORDER BY days_start_date_to_end_date, id) AS next2_start_date,
        COUNT(*) OVER (PARTITION BY holder_id, product_group) AS rows_in_holder_id_product_group
    FROM screened
    WINDOW w_end_date_year_by_start_date AS (PARTITION BY YEAR(end_date) ORDER BY start_date, id)
),
with_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.status,
        s.status_ref,
        s.holder_id_product_group_sum_days_start_date_to_e,
        s.start_date_cume_dist,
        s.next2_start_date,
        s.rows_in_holder_id_product_group,
        p.region AS holder_region,
        p.segment AS holder_segment
    FROM windowed s
    JOIN policyholders p ON p.id = s.holder_id
)
SELECT id, field, metric_value
FROM (
    SELECT id, rows_in_holder_id_product_group, holder_id_product_group_sum_days_start_date_to_e
    FROM with_details_holder
) src
UNPIVOT (
    metric_value FOR field IN (rows_in_holder_id_product_group, holder_id_product_group_sum_days_start_date_to_e)
) unp
