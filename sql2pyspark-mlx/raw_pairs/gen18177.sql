select id, opened_date from stores where sqft between 10165 and 12717 union select id, opened_date from stores where region = 'East';
