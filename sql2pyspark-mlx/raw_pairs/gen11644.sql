WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(status, '/', carrier) AS status_carrier,
        ROUND(cost * 1.07, 2) AS cost_adjusted,
        COALESCE(carrier, 'OTHER') AS resolved_carrier
    FROM shipments
    WHERE delivered_date > DATE '2023-09-25'
      AND order_id IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        carrier,
        shipped_date,
        cost_adjusted,
        resolved_carrier,
        SUM(cost_adjusted) OVER (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_adjusted,
        LEAD(cost_adjusted, 1) OVER w_recent AS next_cost_adjusted,
        ROW_NUMBER() OVER w_grp AS row_seq,
        AVG(cost) OVER w_running AS cost_avg_3_prior
    FROM valid
    WINDOW w_recent AS (PARTITION BY MONTH(delivered_date), carrier ORDER BY delivered_date, id), w_grp AS (PARTITION BY carrier ORDER BY cost DESC, id), w_running AS (ORDER BY delivered_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        cost_adjusted * next_cost_adjusted AS cost_adjusted_times_next_cost_adjusted,
        GREATEST(row_seq, running_cost_adjusted) AS greatest_row_seq_running_cost_adjusted
    FROM enriched
    WHERE shipped_date > TO_DATE('2023-05-07')
      AND carrier IN ('Kerry', 'FedEx', 'Flash')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost_avg_3_prior, id) AS seq_no,
        AVG(cost_avg_3_prior) OVER (ORDER BY running_cost_adjusted, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS cost_avg_3_prior_avg_5_prior
    FROM base
    WHERE shipped_date BETWEEN '2023-09-01' AND '2024-04-21'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY seq_no DESC, id) AS latest_rank
        FROM scored
    ) ranked
    WHERE latest_rank = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        carrier,
        cost_adjusted,
        running_cost_adjusted,
        seq_no,
        cost_avg_3_prior_avg_5_prior,
        latest_rank
    FROM (
        SELECT *, RANK() OVER (PARTITION BY carrier ORDER BY cost_adjusted_times_next_cost_adjusted) AS rank_in_group
        FROM latest
    ) x
    WHERE rank_in_group = 1
),
scored_shipments AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_recent AS event_seq,
        AVG(cost_avg_3_prior_avg_5_prior) OVER (PARTITION BY carrier ORDER BY cost_avg_3_prior_avg_5_prior, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS cost_avg_3_prior_avg_5_prior_avg_5_prior,
        LEAD(cost_avg_3_prior_avg_5_prior, 1) OVER (PARTITION BY carrier ORDER BY latest_rank, id) AS next_cost_avg_3_prior_avg_5_prior,
        PERCENT_RANK() OVER w_part AS cost_avg_3_prior_avg_5_prior_percent_rank
    FROM deduped
    WINDOW w_recent AS (PARTITION BY carrier ORDER BY cost_avg_3_prior_avg_5_prior DESC, id), w_part AS (PARTITION BY carrier ORDER BY cost_avg_3_prior_avg_5_prior DESC NULLS LAST)
),
summary AS (
    -- Multi-level aggregation
    SELECT
        seq_no,
        ROUND(AVG(running_cost_adjusted), 2) AS avg_running_cost_adjusted,
        APPROX_PERCENTILE(next_cost_avg_3_prior_avg_5_prior, ARRAY(0.25, 0.5, 0.75)) AS next_cost_avg_3_prior_avg_5_prior_quartiles,
        COUNT(DISTINCT carrier) AS distinct_carriers
    FROM scored_shipments
    GROUP BY seq_no
)
SELECT
    seq_no,
    avg_running_cost_adjusted,
    next_cost_avg_3_prior_avg_5_prior_quartiles
FROM summary
ORDER BY avg_running_cost_adjusted NULLS LAST
