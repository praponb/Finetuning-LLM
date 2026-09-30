WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        shipped_date,
        cost,
        status,
        CONCAT_WS('-', carrier, status) AS carrier_status_key
    FROM shipments
    WHERE shipped_date > '2024-06-01'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC NULLS LAST, id) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, cost - rank_in_group AS cost_minus_rank_in_group
    FROM deduped
    WHERE shipped_date > TO_DATE('2023-06-27')
      AND status NOT IN ('DELIVERED', 'RETURNED')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY status ORDER BY cost, id) AS cost_bucket,
        SUM(cost_minus_rank_in_group) OVER (PARTITION BY status ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_minus_rank_in_group
    FROM base
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(cost) OVER w_recent AS next_cost,
        LEAD(cost_minus_rank_in_group) OVER w_recent AS next_cost_minus_rank_in_group,
        LAG(cost_minus_rank_in_group) OVER w_recent AS prev_cost_minus_rank_in_group,
        ROW_NUMBER() OVER w_recent AS seq_no,
        COUNT(cost_bucket) OVER (PARTITION BY status) AS status_count_cost_bucket
    FROM metrics
    WINDOW w_recent AS (PARTITION BY status ORDER BY cost_bucket, id)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        MAX(next_cost_minus_rank_in_group) FILTER (WHERE status = 'RETURNED') AS returned_max_next_cost_minus_rank_in_group,
        APPROX_PERCENTILE(next_cost, 0.95) AS next_cost_p95,
        APPROX_PERCENTILE(prev_cost_minus_rank_in_group, 0.5) AS prev_cost_minus_rank_in_group_p50
    FROM scored
    GROUP BY status
)
SELECT status, next_cost_p95
FROM summary
ORDER BY next_cost_p95 DESC NULLS FIRST
