WITH valid AS (
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        SUBSTRING(status, 1, 2) AS status_prefix,
        COALESCE(carrier, 'OTHER') AS carrier_filled
    FROM shipments
    WHERE order_id IS NOT NULL
      AND carrier = 'Kerry'
),
base AS (
    SELECT
        id,
        carrier,
        delivered_date,
        cost,
        status_prefix,
        carrier_filled,
        ROUND(cost * 0.85, 2) AS cost_scaled,
        ROUND(cost, 0) AS cost_rounded,
        COALESCE(carrier, 'DIRECT') AS carrier_clean
    FROM valid
    WHERE delivered_date > TO_DATE('2023-06-11')
      AND carrier IS NOT NULL
),
metrics AS (
    SELECT
        id,
        carrier,
        delivered_date,
        cost,
        status_prefix,
        carrier_filled,
        cost_scaled,
        cost_rounded,
        carrier_clean,
        AVG(cost) OVER (PARTITION BY carrier ORDER BY cost_rounded ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cost_avg_2,
        RANK() OVER (PARTITION BY carrier ORDER BY cost_rounded ASC NULLS FIRST) AS cost_rounded_rank,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date ASC, id ASC) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost ASC, id ASC) AS seq_no,
        FIRST_VALUE(cost_rounded) IGNORE NULLS OVER (PARTITION BY carrier ORDER BY cost_rounded ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost_rounded
    FROM base
),
deduped AS (
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost_avg_2 DESC, id ASC) = 1
)
SELECT
    id,
    delivered_date,
    cost,
    status_prefix,
    cost_scaled,
    cost_rounded,
    carrier_clean,
    cost_avg_2
FROM deduped
ORDER BY cost DESC NULLS LAST;
