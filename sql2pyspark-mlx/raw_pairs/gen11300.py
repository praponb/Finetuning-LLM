from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([3, 6, 11]))
    .select("status")
    .intersect(df_products.select("product_name"))
    .orderBy("status")
)
