from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((F.col("delivered_date") <= "2024-04-20") & (F.col("order_id") == 15) & (F.col("status") != "IN_TRANSIT"))
)

shipments_stats = (
    base
    .groupBy("status")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
)

result = (
    shipments_stats
    .filter(F.col("cost_range") < 52)
    .select("status", "cost_range")
    .orderBy(F.col("status").desc())
)
