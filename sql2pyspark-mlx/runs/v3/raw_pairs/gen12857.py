from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "DELIVERED")
    .groupBy(
        "carrier",
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.min("cost").alias("min_cost"),
    )
    .orderBy(F.col("delivered_month").desc(), "carrier")
    .limit(50)
)
