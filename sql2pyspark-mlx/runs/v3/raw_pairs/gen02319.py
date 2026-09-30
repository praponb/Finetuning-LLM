from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"),
        "status",
    )
    .agg(
        F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("total_cost") >= 520)
    .orderBy("total_cost")
)
