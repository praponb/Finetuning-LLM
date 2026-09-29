from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "premium") & (F.col("category") == "Grocery") & (F.col("price") <= 970))
    .withColumn("price_rounded", F.round("price", 1))
    .withColumn("launch_date_period", F.date_trunc("YEAR", "launch_date"))
    .orderBy(F.col("stock_qty").desc())
)
