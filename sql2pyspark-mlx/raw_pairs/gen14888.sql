WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(location) AS location_length
    FROM departments
    WHERE dept_name = 'Legal'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.level IN ('L5', 'L4')
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY budget DESC, id) AS event_seq,
        CUME_DIST() OVER (PARTITION BY dept_name ORDER BY budget NULLS LAST) AS budget_cume_dist
    FROM eligible
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.full_name LIKE 'K%'
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(event_seq, 1) OVER (ORDER BY location_length, id) AS prev_event_seq,
        MIN(event_seq) OVER w_grp AS dept_name_min_event_seq,
        COUNT(budget) OVER (PARTITION BY dept_name) AS dept_name_count_budget
    FROM kept
    WINDOW w_grp AS (PARTITION BY dept_name, location)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        location,
        CASE WHEN prev_event_seq >= 8 THEN 'HIGH' ELSE 'LOW' END AS prev_event_seq_tier,
        APPROX_PERCENTILE(budget_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS budget_cume_dist_quartiles,
        AVG(prev_event_seq) FILTER (WHERE location = 'Chiang Mai') AS chiang_mai_avg_prev_event_seq,
        AVG(event_seq) FILTER (WHERE location = 'Tokyo') AS tokyo_avg_event_seq
    FROM metrics
    GROUP BY location, prev_event_seq_tier
    HAVING tokyo_avg_event_seq > 0
)
SELECT
    location,
    prev_event_seq_tier,
    budget_cume_dist_quartiles,
    chiang_mai_avg_prev_event_seq,
    tokyo_avg_event_seq
FROM grouped
ORDER BY location;
