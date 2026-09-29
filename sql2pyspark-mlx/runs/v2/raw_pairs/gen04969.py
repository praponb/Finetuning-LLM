from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"))
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.count("cost").alias("cost_count"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
    )
    .filter(F.col("cost_count") > 4)
    .orderBy("delivered_month")
)
