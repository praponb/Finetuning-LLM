from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").isNull())
    .groupBy(
        "status",
        "order_id",
        F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"),
    )
    .agg(F.avg("cost").alias("avg_cost"))
)
