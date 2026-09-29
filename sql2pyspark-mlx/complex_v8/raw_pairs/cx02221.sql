WITH recent_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE move_type IN ('TRANSFER', 'INBOUND')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(qty) OVER (ORDER BY qty DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS qty_sum_1_prior,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY operator), 0), 4) AS unit_price_share_of_operator,
        ROUND(AVG(unit_price) OVER (PARTITION BY operator ORDER BY qty DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_3,
        ROW_NUMBER() OVER (PARTITION BY operator, warehouse_id ORDER BY unit_price, id) AS rn,
        SUM(qty) OVER (PARTITION BY operator) AS operator_sum_qty,
        FIRST_VALUE(qty) OVER (PARTITION BY operator, warehouse_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator, warehouse_id ORDER BY unit_price), 3) AS unit_price_percent_rank,
        MAX(qty) OVER (PARTITION BY operator, warehouse_id ORDER BY unit_price, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS qty_max_3_prior
    FROM recent_moves
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    operator,
    unit_price,
    running_count,
    unit_price_share_of_operator,
    unit_price_avg_3,
    rn,
    unit_price_percent_rank,
    qty_max_3_prior,
    CASE WHEN operator IN ('op_chan', 'op_dew') THEN 'group_a' WHEN operator = 'op_ann' THEN 'group_b' ELSE 'other' END AS operator_group,
    LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug
FROM sequenced
WHERE qty_max_3_prior >= 601
ORDER BY id
LIMIT 5
