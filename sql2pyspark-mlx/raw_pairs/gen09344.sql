WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        DATE_FORMAT(shipped_date, 'yyyy-MM-dd') AS shipped_date_label,
        COALESCE(carrier, 'DIRECT') AS carrier_clean
    FROM shipments
    WHERE carrier IN ('Flash', 'FedEx', 'DHL')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        carrier_clean,
        CASE WHEN carrier = 'Kerry' THEN 1 ELSE 0 END AS is_kerry,
        COALESCE(carrier, 'NONE') AS carrier_filled
    FROM filtered
    WHERE shipped_date > DATE '2023-03-20'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY is_kerry DESC NULLS FIRST, id) AS rank_in_group
    FROM cleaned
    QUALIFY rank_in_group = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(cost, 1, 0) OVER (PARTITION BY carrier ORDER BY is_kerry, id) AS prev_cost,
        LEAD(cost, 1) OVER (ORDER BY delivered_date, id) AS next_cost,
        SUM(is_kerry) OVER (PARTITION BY status, carrier ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_kerry
    FROM ranked
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(next_cost) IGNORE NULLS OVER w_ordered AS last_known_next_cost,
        AVG(next_cost) OVER (PARTITION BY status, carrier ORDER BY delivered_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS next_cost_avg_5_prior
    FROM scored
    WHERE status = 'IN_TRANSIT'
    WINDOW w_ordered AS (PARTITION BY status ORDER BY cumulative_is_kerry, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    carrier,
    kpi,
    value
FROM (
    SELECT
        id,
        carrier,
        cost,
        last_known_next_cost,
        next_cost,
        prev_cost
    FROM enriched
) src
UNPIVOT (
    value FOR kpi IN (cost, last_known_next_cost, next_cost, prev_cost)
);
