from pyspark.sql import functions as F

result = (
    df_products
    .filter(~((F.col("brand") != "Umbrella") & F.col("product_name").like("C%") & (F.col("price") < 830)))
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0)).alias("globex_count"),
        F.max("launch_date").alias("latest_launch_date"),
    )
)
