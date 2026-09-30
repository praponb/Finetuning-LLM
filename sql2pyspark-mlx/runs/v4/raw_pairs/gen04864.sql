WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        balance,
        opened_date,
        status,
        CONCAT_WS('-', status, account_type) AS status_account_type_key,
        COALESCE(status, 'UNKNOWN') AS status_filled
    FROM accounts
    WHERE opened_date >= DATE '2024-04-05'
      AND balance >= 50000
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC NULLS LAST, id) AS rank_in_group
        FROM valid
    ) ranked
    WHERE rank_in_group = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.is_flagged
        )
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        opened_date,
        status,
        status_account_type_key,
        status_filled,
        rank_in_group,
        LAST_DAY(opened_date) AS opened_date_month_end,
        ADD_MONTHS(opened_date, 3) AS opened_date_plus_months
    FROM eligible
    WHERE opened_date > DATE '2024-04-16'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date DESC NULLS LAST, id) AS rn
    FROM filtered
    QUALIFY rn = 1
)
SELECT
    id,
    opened_date,
    status,
    status_account_type_key,
    status_filled,
    rank_in_group,
    opened_date_month_end,
    rn,
    CASE WHEN status RLIKE '^(OPEN|FROZEN)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS status_zone
FROM deduped
