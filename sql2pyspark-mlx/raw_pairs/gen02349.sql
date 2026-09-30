WITH valid AS (
    SELECT *, DATE_SUB(shipped_date, 19) AS shipped_date_minus_19d
    FROM shipments
    WHERE delivered_date >= DATE '2023-06-14'
      AND carrier IS NOT NULL
),
metrics AS (
    SELECT
        *,
        MAX(cost) OVER w_part AS last7_max_cost,
        COUNT(cost) OVER w_part AS last7_count_cost,
        SUM(cost) OVER w_part AS last7_sum_cost,
        AVG(cost) OVER w_part AS last7_avg_cost
    FROM valid
    WINDOW w_part AS (PARTITION BY carrier, order_id ORDER BY shipped_date_minus_19d, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
valid_shipments AS (
    SELECT *, CASE WHEN last7_count_cost >= 15 THEN 'High' WHEN last7_count_cost >= 4 THEN 'Medium' ELSE 'Low' END AS last7_count_cost_tier
    FROM metrics
    WHERE delivered_date > TO_DATE('2023-07-11')
),
scored AS (
    SELECT
        *,
        SUM(last7_avg_cost) OVER w_recent AS running_sum_last7_avg_cost,
        COUNT(last7_avg_cost) OVER w_recent AS running_count_last7_avg_cost,
        AVG(last7_avg_cost) OVER w_recent AS running_avg_last7_avg_cost
    FROM valid_shipments
    WINDOW w_recent AS (PARTITION BY status ORDER BY last7_avg_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    SELECT status, carrier
    FROM scored
    EXCEPT DISTINCT
    SELECT status, carrier
    FROM shipments
    WHERE cost <= 39
),
verified AS (
    SELECT s.*
    FROM scored s
    JOIN kept_keys k ON s.status = k.status AND s.carrier = k.carrier
),
joined AS (
    SELECT
        a.*,
        d.shipping_fee,
        d.channel,
        d.amount
    FROM verified a
    LEFT JOIN orders d
        ON a.order_id <=> d.id
)
SELECT
    channel,
    measure,
    value
FROM (
    SELECT
        channel,
        cost,
        last7_avg_cost,
        last7_sum_cost
    FROM joined
) src
UNPIVOT (
    value FOR measure IN (cost AS COST, last7_avg_cost, last7_sum_cost)
);
