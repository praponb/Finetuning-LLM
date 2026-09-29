WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        CONCAT_WS('/', status, plan) AS status_plan_key,
        WEEKOFYEAR(start_date) AS start_date_weekofyear
    FROM subscriptions
    WHERE end_date > DATE '2024-05-17'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id ASC) AS rn
    FROM valid
    QUALIFY rn = 1
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.country,
        p.city
    FROM deduped a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_hist AS row_seq,
        SUM(monthly_fee) OVER (PARTITION BY country ORDER BY monthly_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee
    FROM enriched
    WINDOW w_hist AS (PARTITION BY country ORDER BY start_date DESC, id ASC)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date DESC NULLS FIRST, id ASC) = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, customer_id
    FROM latest
    INTERSECT
    SELECT id, customer_id
    FROM subscriptions
    WHERE plan = 'premium'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            status,
            customer_id,
            country,
            row_seq
        FROM verified
    ) src
    PIVOT (
        AVG(row_seq) AS amount,
        COUNT(1) AS n
        FOR country IN ('DE' AS de, 'TH' AS th, 'SG' AS sg)
    )
)
SELECT
    status,
    customer_id,
    th_n,
    sg_amount,
    sg_n
FROM wide
ORDER BY status ASC, customer_id ASC
