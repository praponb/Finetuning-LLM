select *, floor(rating) as rating_whole, coalesce(rating, 0) as rating_filled from suppliers;
