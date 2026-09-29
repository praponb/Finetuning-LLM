WITH cleaned AS (
    SELECT
        *,
        CONCAT(status, ', ', carrier) AS status_carrier,
        INITCAP(TRIM(status)) AS status_title,
        COALESCE(carrier, 'OTHER') AS carrier_filled
    FROM shipments
    WHERE delivered_date > DATE '2024-04-07'
      AND order_id IS NOT NULL
      AND MONTH(shipped_date) IN (2, 4, 6)
),
enriched AS (
    SELECT
        *,
        LAST_VALUE(cost) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY cost ASC NULLS FIRST) AS cost_dense_rank
    FROM cleaned
),
enriched_shipments AS (
    SELECT
        *,
        SUM(cost_dense_rank) OVER (PARTITION BY order_id ORDER BY shipped_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_dense_rank,
        AVG(cost_dense_rank) OVER (PARTITION BY status ORDER BY shipped_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS cost_dense_rank_avg_5_prior,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id, status), 4) AS cost_share
    FROM enriched
),
enriched_shipments_2 AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        status_carrier,
        status_title,
        carrier_filled,
        last_known_cost,
        cost_dense_rank,
        cumulative_cost_dense_rank,
        cost_dense_rank_avg_5_prior,
        cost_share,
        AVG(cumulative_cost_dense_rank) OVER (ORDER BY shipped_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS cumulative_cost_dense_rank_avg_5,
        CUME_DIST() OVER (PARTITION BY status ORDER BY cumulative_cost_dense_rank ASC NULLS FIRST) AS cumulative_cost_dense_rank_cume_dist,
        AVG(cumulative_cost_dense_rank) OVER (PARTITION BY order_id) AS order_id_avg_cumulative_cost_dense_rank,
        SUM(cost) OVER (PARTITION BY carrier ORDER BY last_known_cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        CUME_DIST() OVER w_part AS cost_share_cume_dist
    FROM enriched_shipments
    WINDOW w_part AS (ORDER BY cost_share DESC NULLS FIRST)
)
SELECT
    CASE WHEN cost_share_cume_dist >= 0.76 THEN 'LARGE' ELSE 'SMALL' END AS cost_share_cume_dist_tier,
    DATE_TRUNC('YEAR', delivered_date) AS year_start,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    MAX(cost_share) FILTER (WHERE carrier = 'FedEx') AS fedex_max_cost_share,
    GROUPING_ID() AS grouping_id
FROM enriched_shipments_2
GROUP BY ROLLUP (CASE WHEN cost_share_cume_dist >= 0.76 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', delivered_date))
