WITH cleaned AS (
    SELECT
        *,
        ROUND(sqft * 1.07, 2) AS sqft_scaled,
        COALESCE(region, 'N/A') AS region_filled,
        COALESCE(region, 'NONE') AS region_filled_2
    FROM stores
    WHERE opened_date >= DATE '2023-03-27'
      AND sqft >= 12019
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft_scaled DESC, id) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
latest_match AS (
    SELECT
        s.id,
        s.opened_date,
        s.region_filled,
        s.latest_rank,
        top1.units,
        top1.revenue
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.units, c.revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
filtered AS (
    SELECT
        id,
        opened_date,
        region_filled,
        latest_rank,
        revenue,
        COALESCE(revenue, 0) AS revenue_filled
    FROM latest_match
    WHERE opened_date > DATE '2023-04-18'
),
filtered_stores AS (
    SELECT *, LEAST(latest_rank, revenue) AS least_latest_rank_revenue
    FROM filtered
    WHERE opened_date >= DATE '2023-03-28'
      AND revenue <= 2000
)
SELECT
    id,
    region_filled,
    latest_rank,
    revenue,
    revenue_filled
FROM filtered_stores
ORDER BY revenue DESC NULLS FIRST, revenue_filled DESC
