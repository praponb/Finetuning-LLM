from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter((F.col("category") == "Grocery") & F.col("category").isin(["Electronics", "Books"]) & (F.col("stock_qty") >= 138))
)

agg = (
    recent_products
    .groupBy("brand")
    .agg(F.max("launch_date").alias("latest_launch_date"))
)

result = agg.orderBy("brand", "latest_launch_date")
