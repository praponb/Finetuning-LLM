WITH prepared_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        YEAR(closed_at) AS closed_at_year,
        CONCAT(priority, '#', CAST(id AS STRING)) AS priority_ref
    FROM tickets
    WHERE channel = 'phone'
      AND requester_id > 11
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        closed_at_year,
        priority_ref,
        MAX(csat) OVER (ORDER BY closed_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_csat_so_far,
        DENSE_RANK() OVER w_all_by_closed_at_year_ties AS closed_at_year_dense_rank,
        ROUND(AVG(csat) OVER (ORDER BY closed_at_year DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_1_prior,
        csat - LAG(csat) OVER (ORDER BY closed_at_year DESC, id) AS csat_change
    FROM prepared_tickets
    WINDOW w_all_by_closed_at_year_ties AS (ORDER BY closed_at_year DESC)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        DAYOFWEEK(opened_at) AS opened_at_dayofweek,
        ROUND(csat_avg_1_prior * 1.5, 2) AS csat_avg_1_prior_adj
    FROM sequenced
),
latest_per_key AS (
    -- Keep the latest row per channel, agent_id
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        closed_at_year,
        priority_ref,
        max_csat_so_far,
        closed_at_year_dense_rank,
        csat_avg_1_prior,
        csat_change,
        opened_at_dayofweek,
        csat_avg_1_prior_adj
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel, agent_id ORDER BY csat_change DESC, id DESC) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        PERCENTILE_APPROX(closed_at_year, 0.5) AS median_closed_at_year,
        COUNT(*) AS cnt,
        ROUND(AVG(csat_avg_1_prior_adj), 2) AS avg_csat_avg_1_prior_adj,
        MIN(closed_at) AS first_closed_at,
        MAX(opened_at_dayofweek) AS max_opened_at_dayofweek,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids
    FROM latest_per_key
    GROUP BY DATE_TRUNC('MONTH', opened_at)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM totals
    WHERE cnt > (SELECT AVG(cnt) FROM totals)
)
-- Subtotals with ROLLUP
SELECT
    median_closed_at_year,
    distinct_agent_ids,
    cnt,
    GROUPING(median_closed_at_year) AS is_total_median_closed_at_year,
    MIN(median_closed_at_year) AS min_median_closed_at_year,
    MIN(cnt) AS min_cnt,
    ROUND(SUM(avg_csat_avg_1_prior_adj) / NULLIF(SUM(cnt), 0), 3) AS avg_csat_avg_1_prior_adj_to_cnt_ratio
FROM with_activity
GROUP BY ROLLUP(median_closed_at_year, distinct_agent_ids, cnt)
ORDER BY median_closed_at_year NULLS LAST, distinct_agent_ids NULLS LAST, cnt NULLS LAST
