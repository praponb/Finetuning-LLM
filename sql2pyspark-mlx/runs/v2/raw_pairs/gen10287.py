from pyspark.sql import functions as F

recent_products = df_products.filter(F.col("supplier_id").isNotNull() & (F.col("brand") == "Stark"))

brand_summary = (
    recent_products
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("category") == "Home", F.col("cost")).otherwise(0)).alias("home_cost"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
)

result = brand_summary.select("brand", "home_cost")
