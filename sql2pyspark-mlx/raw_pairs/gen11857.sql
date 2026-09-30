WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(balance AS INT) AS balance_int,
        DATE_TRUNC('WEEK', opened_date) AS opened_date_period,
        COALESCE(status, 'UNKNOWN') AS status_filled
    FROM accounts
    WHERE opened_date > DATE '2023-05-22'
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.account_type,
        src.balance,
        src.status,
        p.signup_date,
        p.country
    FROM valid src
    LEFT JOIN customers p
        ON src.customer_id <=> p.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM enriched
    EXCEPT
    SELECT id
    FROM accounts
    WHERE customer_id IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.id = k.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY country ORDER BY balance) AS balance_cume_dist,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date), country ORDER BY balance DESC NULLS FIRST) AS balance_dense_rank,
        RANK() OVER (ORDER BY balance) AS balance_rank,
        DENSE_RANK() OVER (PARTITION BY country, status ORDER BY balance NULLS LAST) AS balance_dense_rank_2
    FROM reconciled
),
matched_keys AS (
    -- Set difference on keys
    SELECT status
    FROM scored
    EXCEPT
    SELECT status
    FROM accounts
    WHERE balance > 33000
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.status = k.status
),
scored_accounts AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(balance_rank) OVER (PARTITION BY account_type) AS account_type_min_balance_rank,
        CUME_DIST() OVER (ORDER BY balance_dense_rank_2 DESC NULLS LAST) AS balance_dense_rank_2_cume_dist,
        RANK() OVER (PARTITION BY country ORDER BY balance_dense_rank DESC NULLS LAST) AS balance_dense_rank_rank,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date), country ORDER BY balance NULLS FIRST) AS balance_rank_2,
        COUNT(balance_dense_rank_2) OVER (PARTITION BY customer_id) AS customer_id_count_balance_dense_rank_2
    FROM verified
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN balance_dense_rank_2_cume_dist >= 0.8 THEN 'LARGE' ELSE 'SMALL' END AS balance_dense_rank_2_cume_dist_tier,
        ROUND(SUM(balance_dense_rank) / NULLIF(COUNT(CASE WHEN country = 'TH' THEN 1 END), 0), 2) AS balance_dense_rank_per_th,
        COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) AS closed_count
    FROM scored_accounts
    GROUP BY CASE WHEN balance_dense_rank_2_cume_dist >= 0.8 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    balance_dense_rank_2_cume_dist_tier,
    balance_dense_rank_per_th,
    closed_count
FROM grouped
