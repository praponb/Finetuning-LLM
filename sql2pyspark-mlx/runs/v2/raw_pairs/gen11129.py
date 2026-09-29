from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") <= 750) & (F.col("brand") == "Umbrella") & F.col("category").isin(["Home", "Electronics"]))
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"), "category")
    .agg(
        F.count("*").alias("total_count"),
        F.count("price").alias("price_count"),
    )
    .orderBy("total_count", F.col("price_count").desc(), "launch_month", "category")
    .limit(50)
)
