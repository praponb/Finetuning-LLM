from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((~F.col("carrier").eqNullSafe(F.lit("Kerry"))) & (F.col("carrier") == "Kerry"))
    .withColumn("status_group", F.when(F.col("status").isin(["DELIVERED", "LOST"]), "Priority").otherwise("Other"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("DIRECT")))
)

result = (
    filtered
    .groupBy("carrier")
    .pivot("status", ["RETURNED", "LOST", "DELIVERED"])
    .agg(F.max("cost").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
