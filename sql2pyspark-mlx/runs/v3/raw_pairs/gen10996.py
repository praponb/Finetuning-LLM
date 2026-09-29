from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"), "category")
    .agg(
        F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0)).alias("umbrella_count"),
    )
)
