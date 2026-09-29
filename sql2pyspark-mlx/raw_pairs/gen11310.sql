select id, country, city, rating, initcap(trim(country)) as country_title from suppliers where city = 'New York' or city = 'Berlin' order by country asc, rating desc;
