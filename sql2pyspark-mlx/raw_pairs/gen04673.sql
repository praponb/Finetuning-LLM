WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        DATEDIFF(start_date, end_date) days_end_date_to_start_date
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY days_end_date_to_start_date NULLS FIRST, id) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER w_recent days_end_date_to_start_date_percent_rank,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), customer_id ORDER BY days_end_date_to_start_date DESC NULLS FIRST) days_end_date_to_start_date_rank,
        FIRST_VALUE(days_end_date_to_start_date) IGNORE NULLS OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_days_end_date_to_start_date,
        ROW_NUMBER() OVER w_recent_2 row_seq,
        AVG(days_end_date_to_start_date) OVER (PARTITION BY status, plan ORDER BY days_end_date_to_start_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) days_end_date_to_start_date_avg_6_prior
    FROM deduped
    WINDOW w_recent AS (PARTITION BY customer_id ORDER BY days_end_date_to_start_date DESC), w_recent_2 AS (PARTITION BY customer_id ORDER BY start_date, id)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (ORDER BY days_end_date_to_start_date_percent_rank, id) days_end_date_to_start_date_percent_rank_bucket,
        LEAD(days_end_date_to_start_date_avg_6_prior, 2) OVER (PARTITION BY YEAR(start_date), plan ORDER BY days_end_date_to_start_date_rank, id) next_days_end_date_to_start_date_avg_6_prior
    FROM metrics
),
metrics_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        start_date,
        end_date,
        status,
        days_end_date_to_start_date,
        row_num,
        days_end_date_to_start_date_percent_rank,
        days_end_date_to_start_date_rank,
        days_end_date_to_start_date_avg_6_prior,
        days_end_date_to_start_date_percent_rank_bucket,
        LEAD(days_end_date_to_start_date_rank, 2) OVER (PARTITION BY status ORDER BY first_known_days_end_date_to_start_date, id) next_days_end_date_to_start_date_rank,
        SUM(days_end_date_to_start_date_percent_rank) OVER (PARTITION BY YEAR(end_date), status ORDER BY first_known_days_end_date_to_start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_days_end_date_to_start_date_percent_rank,
        SUM(next_days_end_date_to_start_date_avg_6_prior) OVER (PARTITION BY YEAR(start_date), customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_next_days_end_date_to_start_date_avg_6_prior
    FROM enriched
)
SELECT
    id,
    end_date,
    days_end_date_to_start_date_percent_rank,
    days_end_date_to_start_date_rank,
    days_end_date_to_start_date_percent_rank_bucket,
    next_days_end_date_to_start_date_rank,
    running_days_end_date_to_start_date_percent_rank,
    cumulative_next_days_end_date_to_start_date_avg_6_prior
FROM metrics_subscriptions
