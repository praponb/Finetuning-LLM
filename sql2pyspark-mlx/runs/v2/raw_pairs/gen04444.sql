select city, 'supplier' as source from suppliers union select city, 'store' as source from stores where city = 'Bangkok'
