WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id, dept_id ORDER BY hire_date DESC NULLS FIRST, id) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        email,
        salary,
        hire_date,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date, id) AS event_seq
    FROM ranked
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        seq_no,
        SUM(event_seq) OVER w_running AS running_event_seq,
        NTILE(5) OVER (ORDER BY event_seq DESC, id) AS event_seq_bucket
    FROM metrics
    WINDOW w_running AS (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_employees AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (ORDER BY seq_no DESC, id) AS seq_no_bucket,
        ROW_NUMBER() OVER (ORDER BY seq_no, id) AS seq_no_2,
        AVG(event_seq_bucket) OVER (ORDER BY event_seq_bucket, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS event_seq_bucket_avg_2,
        SUM(event_seq_bucket) OVER (ORDER BY event_seq_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_event_seq_bucket,
        AVG(event_seq_bucket) OVER (ORDER BY event_seq_bucket, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS event_seq_bucket_avg_5_prior
    FROM scored
),
scored_employees AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        seq_no,
        running_event_seq,
        seq_no_2,
        event_seq_bucket_avg_2,
        cumulative_event_seq_bucket,
        event_seq_bucket_avg_5_prior,
        CUME_DIST() OVER (ORDER BY cumulative_event_seq_bucket DESC NULLS FIRST) AS cumulative_event_seq_bucket_cume_dist,
        PERCENT_RANK() OVER (ORDER BY event_seq_bucket_avg_5_prior DESC NULLS FIRST) AS event_seq_bucket_avg_5_prior_percent_rank
    FROM metrics_employees
)
-- Window metrics over several different partitions
SELECT
    id,
    cumulative_event_seq_bucket,
    event_seq_bucket_avg_5_prior,
    cumulative_event_seq_bucket_cume_dist,
    event_seq_bucket_avg_5_prior_percent_rank,
    LEAD(running_event_seq) OVER (ORDER BY cumulative_event_seq_bucket_cume_dist, id) AS next_running_event_seq,
    LEAD(event_seq_bucket_avg_2) OVER (ORDER BY cumulative_event_seq_bucket_cume_dist, id) AS next_event_seq_bucket_avg_2,
    ROW_NUMBER() OVER (ORDER BY cumulative_event_seq_bucket_cume_dist, id) AS event_seq,
    SUM(running_event_seq) OVER (ORDER BY cumulative_event_seq_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_event_seq
FROM scored_employees;
