from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("carrier").isin(["UPS", "DHL"]), "core").otherwise("other").alias("carrier_group"),
        "status",
    )
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .filter(F.col("cost_range") > 79)
    .select("carrier_group", "status", "cost_range")
)
