from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter((F.year("delivered_date") >= 2025) & F.month("shipped_date").isin([2, 7, 9]))
    .withColumn("status_upper", F.upper("status"))
)

agg = (
    recent_shipments
    .groupBy("status", F.date_trunc("MONTH", "delivered_date").alias("delivered_month"))
    .agg(F.countDistinct("cost").alias("unique_orders"))
)

result = (
    agg
    .select("status", "delivered_month", "unique_orders")
    .orderBy(F.col("status").desc(), "delivered_month")
    .limit(1)
)
