WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('/', location, dept_name) AS location_dept_name_key,
        CASE WHEN location = 'Tokyo' THEN 1 ELSE 0 END AS is_tokyo,
        COALESCE(dept_name, 'OTHER') AS dept_name_filled
    FROM departments
    WHERE location IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        budget,
        location_dept_name_key,
        FIRST_VALUE(budget, TRUE) OVER (PARTITION BY location ORDER BY is_tokyo, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_budget,
        NTILE(4) OVER (PARTITION BY location ORDER BY budget DESC, id) AS budget_bucket,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY is_tokyo DESC, id) AS event_seq,
        NTILE(3) OVER (PARTITION BY dept_name ORDER BY is_tokyo, id) AS is_tokyo_bucket,
        LEAD(is_tokyo, 1) OVER (ORDER BY is_tokyo, id) AS next_is_tokyo
    FROM valid
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        budget,
        first_known_budget,
        budget_bucket,
        event_seq,
        next_is_tokyo,
        is_tokyo_bucket + budget_bucket AS is_tokyo_bucket_plus_budget_bucket
    FROM metrics
    WHERE first_known_budget BETWEEN 3042000 AND 3328000
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(budget) OVER (ORDER BY first_known_budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS budget_avg_3,
        LEAD(next_is_tokyo, 1) OVER (ORDER BY budget_bucket, id) AS next_next_is_tokyo,
        CUME_DIST() OVER (ORDER BY event_seq DESC NULLS LAST) AS event_seq_cume_dist,
        AVG(event_seq) OVER (ORDER BY event_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS event_seq_avg_3,
        NTILE(3) OVER (ORDER BY next_is_tokyo, id) AS next_is_tokyo_bucket
    FROM cleaned
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(first_known_budget) OVER (ORDER BY next_is_tokyo, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_first_known_budget,
        SUM(first_known_budget) OVER (ORDER BY next_is_tokyo, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_first_known_budget,
        MAX(first_known_budget) OVER (ORDER BY next_is_tokyo, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_first_known_budget,
        COUNT(first_known_budget) OVER (ORDER BY next_is_tokyo, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_first_known_budget
    FROM enriched
)
-- Grouped metrics
SELECT
    CASE WHEN event_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
    CASE WHEN running_min_first_known_budget >= 3317000 THEN 'HIGH' ELSE 'LOW' END AS running_min_first_known_budget_tier,
    PERCENTILE_APPROX(next_is_tokyo, ARRAY(0.25, 0.5, 0.75)) AS next_is_tokyo_quartiles,
    COUNT(*) AS num_rows,
    COUNT(*) AS row_count
FROM scored
GROUP BY CUBE (CASE WHEN event_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN running_min_first_known_budget >= 3317000 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY num_rows DESC NULLS FIRST
