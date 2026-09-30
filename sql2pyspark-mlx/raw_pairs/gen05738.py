from pyspark.sql import functions as F

base = df_shipments.filter(F.col("carrier").isin(["DHL", "Flash", "UPS"]))

summary = (
    base
    .groupBy("order_id")
    .agg(
        F.count("*").alias("cnt"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)

result = (
    summary
    .select("order_id", "cnt", "cost_range")
    .orderBy(F.col("order_id").desc())
)
