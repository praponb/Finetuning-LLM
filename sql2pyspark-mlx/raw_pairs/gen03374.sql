WITH deduped AS (
    SELECT
        id,
        customer_id,
        account_type,
        status
    FROM accounts
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC, id) = 1
),
joined AS (
    SELECT
        e.id,
        e.customer_id,
        e.status,
        p.name,
        p.age
    FROM deduped e
    INNER JOIN customers p
        ON e.customer_id = p.id
),
base AS (
    SELECT
        id,
        customer_id,
        name,
        age,
        INITCAP(TRIM(name)) AS name_title,
        COALESCE(status, 'DIRECT') AS resolved_status
    FROM joined
    WHERE status IS NOT NULL
      AND status IS NOT DISTINCT FROM 'CLOSED'
),
kept_keys AS (
    SELECT customer_id
    FROM base
    INTERSECT
    SELECT customer_id
    FROM accounts
    WHERE balance < 73000
),
kept_rows AS (
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
)
SELECT
    dim.city,
    dim.country,
    COUNT(*) AS rows_total,
    AVG(s.age) AS avg_age
FROM kept_rows s
LEFT JOIN customers dim ON s.customer_id = dim.id
GROUP BY GROUPING SETS ((dim.city, dim.country), (dim.city), ());
