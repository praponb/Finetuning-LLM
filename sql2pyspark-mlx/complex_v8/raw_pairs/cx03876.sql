WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.unit_price) AS prior_unit_price_3d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        prior_unit_price_3d - LAG(prior_unit_price_3d) OVER (PARTITION BY prior_rows_3d, MONTH(moved_at) ORDER BY prior_unit_price_3d, id) AS prior_unit_price_3d_change,
        ROUND(AVG(prior_unit_price_3d) OVER (PARTITION BY prior_rows_3d, MONTH(moved_at) ORDER BY prior_unit_price_3d, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS prior_unit_price_3d_avg_7,
        MIN(prior_unit_price_3d) OVER (PARTITION BY sku_id) AS sku_id_min_prior_unit_price_3d,
        ROW_NUMBER() OVER w_moved_at_month_sku_id_by_pri AS rn,
        LEAD(moved_at) OVER w_prior_rows_3d_moved_at_month AS next_moved_at
    FROM joined_ranges
    WINDOW w_moved_at_month_sku_id_by_pri AS (PARTITION BY DATE_TRUNC('MONTH', moved_at), sku_id ORDER BY prior_rows_3d, id), w_prior_rows_3d_moved_at_month AS (PARTITION BY prior_rows_3d, MONTH(moved_at) ORDER BY prior_unit_price_3d, id)
),
deduped AS (
    -- Keep the latest row per sku_id
    SELECT
        id,
        sku_id,
        moved_at,
        prior_rows_3d,
        prior_unit_price_3d,
        prior_unit_price_3d_change,
        prior_unit_price_3d_avg_7,
        sku_id_min_prior_unit_price_3d,
        rn,
        next_moved_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY sku_id_min_prior_unit_price_3d DESC, id DESC) AS recency_rank
        FROM with_trends
    ) ranked
    WHERE recency_rank = 1
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(prior_unit_price_3d * 0.9, 2) AS prior_unit_price_3d_adj,
        TO_DATE(moved_at) AS moved_at_date
    FROM deduped
)
SELECT
    id,
    sku_id,
    moved_at,
    prior_rows_3d,
    prior_unit_price_3d,
    prior_unit_price_3d_change,
    prior_unit_price_3d_avg_7,
    sku_id_min_prior_unit_price_3d,
    rn,
    next_moved_at,
    prior_unit_price_3d_adj,
    moved_at_date,
    prior_unit_price_3d_adj - prior_rows_3d AS prior_unit_price_3d_adj_minus_prior_rows_3d,
    DATE_ADD(moved_at_date, 90) AS moved_at_date_plus_90d
FROM annotated
WHERE rn > 3
ORDER BY prior_unit_price_3d_change DESC, sku_id DESC
