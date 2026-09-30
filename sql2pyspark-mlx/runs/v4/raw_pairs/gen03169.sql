WITH base AS (
    SELECT
        id,
        order_id,
        shipped_date,
        DATE_ADD(delivered_date, 61) AS delivered_date_plus_61d,
        ROUND(cost * 100, 2) AS cost_adjusted
    FROM shipments
    WHERE shipped_date > TO_DATE('2024-02-18')
      AND order_id IS NOT NULL
      AND carrier = 'UPS'
),
metrics AS (
    SELECT
        *,
        AVG(cost_adjusted) OVER (PARTITION BY order_id) AS order_id_avg_cost_adjusted,
        AVG(cost_adjusted) OVER (ORDER BY cost_adjusted, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS cost_adjusted_avg_5,
        CUME_DIST() OVER (ORDER BY cost_adjusted NULLS FIRST) AS cost_adjusted_cume_dist
    FROM base
),
cleaned AS (
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date_plus_61d,
        cost_adjusted,
        order_id_avg_cost_adjusted,
        cost_adjusted_avg_5,
        cost_adjusted_cume_dist,
        ROUND(cost_adjusted_avg_5, 2) AS cost_adjusted_avg_5_rounded
    FROM metrics
    WHERE shipped_date > TO_DATE('2023-10-12')
      AND order_id IS NOT NULL
      AND delivered_date_plus_61d < '2024-07-01'
),
metrics_shipments AS (
    SELECT
        *,
        SUM(cost_adjusted_avg_5) OVER (PARTITION BY order_id ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_adjusted_avg_5,
        SUM(cost_adjusted_avg_5_rounded) OVER (PARTITION BY order_id ORDER BY order_id_avg_cost_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_adjusted_avg_5_rounded,
        FIRST_VALUE(cost_adjusted) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY delivered_date_plus_61d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost_adjusted
    FROM cleaned
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost_adjusted_cume_dist DESC NULLS FIRST, id) AS row_num
    FROM metrics_shipments
    QUALIFY row_num = 1
),
latest AS (
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC NULLS LAST, id) = 1
)
SELECT
    *,
    LAST_VALUE(first_known_cost_adjusted) IGNORE NULLS OVER (ORDER BY cumulative_cost_adjusted_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_known_cost_adjusted,
    RANK() OVER w_ordered AS order_id_avg_cost_adjusted_rank
FROM latest
WINDOW w_ordered AS (PARTITION BY order_id ORDER BY order_id_avg_cost_adjusted NULLS LAST);
