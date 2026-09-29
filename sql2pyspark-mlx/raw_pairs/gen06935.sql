WITH cleaned AS (
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        ROUND(DATEDIFF(shipped_date, delivered_date) / 30, 1) AS months_delivered_date_to_shipped_date,
        COALESCE(carrier, 'DIRECT') AS resolved_carrier
    FROM shipments
    WHERE cost > 60
),
enriched AS (
    SELECT
        *,
        LEAD(cost, 2, 0) OVER w_recent AS next_cost,
        MAX(cost) OVER (PARTITION BY order_id) AS order_id_max_cost,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY cost NULLS LAST) AS cost_percent_rank,
        AVG(months_delivered_date_to_shipped_date) OVER (PARTITION BY MONTH(shipped_date) ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS months_delivered_date_to_shipped_date_avg_2_prior,
        FIRST_VALUE(months_delivered_date_to_shipped_date) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_months_delivered_date_to_shipped_date
    FROM cleaned
    WHERE YEAR(shipped_date) >= 2024
    WINDOW w_recent AS (ORDER BY months_delivered_date_to_shipped_date, id)
),
with_windows AS (
    SELECT
        *,
        COUNT(months_delivered_date_to_shipped_date_avg_2_prior) OVER w_grp AS running_count_months_delivered_date_to_shipped_date_avg_2_prior,
        AVG(months_delivered_date_to_shipped_date_avg_2_prior) OVER w_grp AS running_avg_months_delivered_date_to_shipped_date_avg_2_prior,
        SUM(months_delivered_date_to_shipped_date_avg_2_prior) OVER w_grp AS running_sum_months_delivered_date_to_shipped_date_avg_2_prior,
        AVG(months_delivered_date_to_shipped_date) OVER w_ordered AS months_delivered_date_to_shipped_date_avg_2
    FROM enriched
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
enriched_shipments AS (
    SELECT
        *,
        COUNT(running_avg_months_delivered_date_to_shipped_date_avg_2_prior) OVER w_grp AS running_count_running_avg_months_delivered_date_to_shipped_date_avg_2_prior,
        MAX(running_avg_months_delivered_date_to_shipped_date_avg_2_prior) OVER w_grp AS running_max_running_avg_months_delivered_date_to_shipped_date_avg_2_prior,
        SUM(running_avg_months_delivered_date_to_shipped_date_avg_2_prior) OVER w_grp AS running_sum_running_avg_months_delivered_date_to_shipped_date_avg_2_prior
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY first_known_months_delivered_date_to_shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    SELECT
        *,
        FLOOR(running_count_running_avg_months_delivered_date_to_shipped_date_avg_2_prior) AS running_count_running_avg_months_delivered_date_to_shipped_date_avg_2_prior_whole,
        running_sum_months_delivered_date_to_shipped_date_avg_2_prior + running_count_months_delivered_date_to_shipped_date_avg_2_prior AS running_sum_months_delivered_date_to_shipped_date_avg_2_prior_plus_running_count_months_delivered_date_to_shipped_date_avg_2_prior
    FROM enriched_shipments
    WHERE delivered_date >= DATE '2023-08-10'
),
metrics AS (
    SELECT
        *,
        ROUND(running_count_months_delivered_date_to_shipped_date_avg_2_prior / SUM(running_count_months_delivered_date_to_shipped_date_avg_2_prior) OVER (PARTITION BY order_id), 4) AS running_count_months_delivered_date_to_shipped_date_avg_2_prior_share,
        AVG(order_id_max_cost) OVER (ORDER BY running_sum_months_delivered_date_to_shipped_date_avg_2_prior_plus_running_count_months_delivered_date_to_shipped_date_avg_2_prior, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS order_id_max_cost_avg_2_prior
    FROM base
),
filtered AS (
    SELECT *, running_count_running_avg_months_delivered_date_to_shipped_date_avg_2_prior - running_sum_running_avg_months_delivered_date_to_shipped_date_avg_2_prior AS running_count_running_avg_months_delivered_date_to_shipped_date_avg_2_prior_minus_running_sum_running_avg_months_delivered_date_to_shipped_date_avg_2_prior
    FROM metrics
    WHERE shipped_date > DATE '2023-06-03'
      AND order_id IS NOT NULL
)
SELECT
    id,
    shipped_date,
    cost,
    months_delivered_date_to_shipped_date,
    resolved_carrier,
    next_cost,
    order_id_max_cost,
    cost_percent_rank,
    months_delivered_date_to_shipped_date_avg_2_prior
FROM filtered
ORDER BY order_id_max_cost;
