from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select("*", F.substring("carrier", 1, 3).alias("carrier_prefix"))
)

result = (
    base
    .groupBy("status")
    .pivot("carrier", ["Flash", "DHL", "UPS"])
    .agg(F.avg("cost").alias("value_sum"))
)
