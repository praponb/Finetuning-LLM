WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC NULLS LAST) AS dedup_rank
        FROM transactions
    ) x
    WHERE dedup_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS amount_bucket,
        CUME_DIST() OVER (PARTITION BY MONTH(txn_time), txn_type ORDER BY amount DESC NULLS LAST) AS amount_cume_dist,
        AVG(amount) OVER w_hist AS amount_avg_6_prior
    FROM deduped
    WINDOW w_hist AS (PARTITION BY MONTH(txn_time), merchant ORDER BY txn_time, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT account_id, id
    FROM scored
    EXCEPT DISTINCT
    SELECT account_id, id
    FROM transactions
    WHERE amount >= 1000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.account_id = k.account_id AND s.id = k.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT merchant, txn_type
    FROM kept_rows
    EXCEPT ALL
    SELECT merchant, txn_type
    FROM transactions
    WHERE amount BETWEEN 1900 AND 2000
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN kept_keys k ON s.merchant = k.merchant AND s.txn_type = k.txn_type
),
enriched AS (
    -- Ranking and gap features
    SELECT
        is_flagged,
        amount_bucket,
        CUME_DIST() OVER (PARTITION BY MONTH(txn_time), merchant ORDER BY amount_bucket DESC) AS amount_bucket_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY MONTH(txn_time), account_id ORDER BY amount_avg_6_prior DESC NULLS LAST) AS amount_avg_6_prior_percent_rank,
        PERCENT_RANK() OVER (ORDER BY amount DESC) AS amount_percent_rank,
        RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', txn_time), txn_type ORDER BY amount_avg_6_prior NULLS FIRST) AS amount_avg_6_prior_rank,
        RANK() OVER (ORDER BY amount_cume_dist) AS amount_cume_dist_rank
    FROM reconciled
    WHERE merchant IN ('Lazada', 'Starbucks')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(amount_bucket_cume_dist, 0) AS amount_bucket_cume_dist_filled
    FROM enriched
    WHERE amount_avg_6_prior_rank <= 8
),
base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(amount_bucket_cume_dist_filled * 1.15, 2) AS amount_bucket_cume_dist_filled_adjusted
    FROM cleaned
    WHERE amount_cume_dist_rank < 3
)
SELECT
    amount_bucket_cume_dist,
    amount_percent_rank,
    amount_cume_dist_rank,
    amount_bucket_cume_dist_filled
FROM base
