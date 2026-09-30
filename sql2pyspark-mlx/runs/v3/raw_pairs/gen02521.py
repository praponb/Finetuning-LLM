from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "carrier",
        F.date_trunc("MONTH", "delivered_date").alias("delivered_month"),
    )
    .agg(F.count("cost").alias("cost_count"))
    .filter(F.col("cost_count") > 4)
    .select("carrier", "delivered_month", "cost_count")
    .orderBy(F.col("cost_count").desc(), "delivered_month")
)
