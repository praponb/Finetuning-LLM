WITH latest AS (SELECT * FROM stores QUALIFY ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY sqft ASC, id ASC) = 1) SELECT id, store_name, region FROM latest ORDER BY id DESC NULLS FIRST
