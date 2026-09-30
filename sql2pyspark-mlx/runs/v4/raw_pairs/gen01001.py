from pyspark.sql import functions as F

result = (
    df_products
    .withColumn("week_start", F.date_trunc("week", "launch_date"))
    .groupingSets([["category", "week_start"], ["category"]], "category", "week_start")
    .agg(
        F.max(F.when(F.col("category") == "Fashion", F.col("price"))).alias("fashion_max_price"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("stock_qty_bit_or").asc_nulls_last())
)
