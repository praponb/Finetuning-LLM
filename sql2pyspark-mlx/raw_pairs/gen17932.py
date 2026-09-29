from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(440, 620) & F.col("price").between(320, 710))
    .groupBy("brand", F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0)).alias("initech_count"),
    )
    .filter(F.col("initech_count") < 36)
)
