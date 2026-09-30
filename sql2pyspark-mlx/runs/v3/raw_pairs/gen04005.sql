select rating, city, supplier_name, coalesce(rating, 0) as rating_filled from suppliers where country = 'US' and rating >= 3.7 and rating is null
