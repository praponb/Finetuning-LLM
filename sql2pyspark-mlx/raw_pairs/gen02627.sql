SELECT *, RANK() OVER (PARTITION BY promo_code ORDER BY starts_on DESC) AS row_num FROM promotions QUALIFY row_num = 1
