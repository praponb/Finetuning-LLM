from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") >= 27) & ~F.col("carrier").isin(["DHL", "UPS", "Kerry"]) & (F.col("delivered_date") < "2025-05-01"))
    .groupBy("carrier", F.month("shipped_date").alias("shipped_month"))
    .agg(F.countDistinct("cost").alias("unique_orders"))
    .orderBy(F.col("carrier").desc(), "shipped_month")
    .limit(3)
)
