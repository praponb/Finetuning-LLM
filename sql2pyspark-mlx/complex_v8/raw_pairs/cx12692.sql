WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        DATE_TRUNC('WEEK', opened_at) AS opened_at_week
    FROM tickets
    WHERE requester_id <= 20
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        opened_at,
        closed_at,
        csat,
        opened_at_week,
        LAG(csat) OVER (PARTITION BY agent_id ORDER BY closed_at ASC, id) AS prev_csat,
        LEAD(closed_at) OVER w_agent_id_by_closed_at AS next_closed_at,
        ROUND(csat / NULLIF(SUM(csat) OVER w_csat, 0), 4) AS csat_share_of_csat,
        LAG(opened_at_week) OVER (PARTITION BY agent_id ORDER BY closed_at ASC, id) AS prev_opened_at_week,
        FIRST_VALUE(requester_id) OVER (PARTITION BY agent_id ORDER BY closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id
    FROM base
    WINDOW w_agent_id_by_closed_at AS (PARTITION BY agent_id ORDER BY closed_at ASC, id), w_csat AS (PARTITION BY csat)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        opened_at,
        closed_at,
        csat,
        opened_at_week,
        next_closed_at,
        csat_share_of_csat,
        first_requester_id,
        LEAD(opened_at) OVER (ORDER BY requester_id, id) AS next_opened_at,
        SUM(requester_id) OVER (ORDER BY prev_opened_at_week DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS requester_id_sum_2_prior
    FROM with_trends
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM metrics
    WHERE csat_share_of_csat > (SELECT AVG(csat_share_of_csat) FROM metrics)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        requester_id,
        opened_at,
        closed_at,
        csat,
        next_closed_at,
        csat_share_of_csat,
        first_requester_id,
        requester_id_sum_2_prior,
        ROUND(csat / NULLIF(first_requester_id, 0), 3) AS csat_per_first_requester_id,
        DAYOFWEEK(opened_at) AS opened_at_dayofweek,
        TO_DATE(closed_at) AS closed_at_date
    FROM with_activity
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', next_closed_at) ORDER BY csat, id), 3) AS csat_cume_dist,
        LEAD(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', next_closed_at) ORDER BY csat, id) AS next_requester_id,
        ROUND(first_requester_id / NULLIF(SUM(first_requester_id) OVER w_closed_at_date_month, 0), 4) AS first_requester_id_share_of_closed_at_date_month,
        RANK() OVER w_csat_opened_at_dayofweek_by AS closed_at_rank,
        ROUND(first_requester_id - AVG(first_requester_id) OVER (PARTITION BY MONTH(closed_at_date)), 2) AS first_requester_id_vs_closed_at_date_month_avg,
        NTH_VALUE(csat_per_first_requester_id, 2) OVER (PARTITION BY csat, opened_at_dayofweek ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat_per_first_requester_id
    FROM derived
    WINDOW w_closed_at_date_month AS (PARTITION BY MONTH(closed_at_date)), w_csat_opened_at_dayofweek_by AS (PARTITION BY csat, opened_at_dayofweek ORDER BY closed_at DESC)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        closed_at,
        csat,
        next_closed_at,
        first_requester_id,
        closed_at_date,
        csat_cume_dist,
        next_requester_id,
        first_requester_id_vs_closed_at_date_month_avg,
        QUARTER(closed_at_date) AS closed_at_date_quarter,
        DATEDIFF(DATE '2026-01-01', closed_at) AS days_since_closed_at,
        DATEDIFF(DATE '2025-12-31', closed_at) AS days_since_closed_at_2
    FROM ranked
    WHERE first_requester_id_share_of_closed_at_date_month > 0.1
)
SELECT DISTINCT csat, closed_at_date_quarter, closed_at_date
FROM enriched
ORDER BY closed_at_date_quarter DESC NULLS FIRST
