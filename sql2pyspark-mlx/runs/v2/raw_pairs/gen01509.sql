select id, brand, price, round(price / sum(price) over (partition by brand), 3) as price_share from products where year(launch_date) = 2025 order by price_share desc;
