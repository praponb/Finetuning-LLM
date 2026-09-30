select id, sqft from stores where sqft <= 9190 union select id, sqft from stores where city in ('New York', 'Berlin');
