SELECT id, sqft, region, DENSE_RANK() OVER (ORDER BY sqft) dense_rnk, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) sqft_row FROM stores;
