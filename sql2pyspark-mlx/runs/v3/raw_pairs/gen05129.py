from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((F.col("order_id") == 4) & ((F.col("status") != "DELIVERED") | (F.col("cost") <= 48)))
)

summary = (
    base
    .groupBy("carrier")
    .agg(F.max("cost").alias("max_cost"))
)

result = (
    summary
    .filter(F.col("max_cost") > 66)
    .select("carrier", "max_cost")
    .orderBy(F.col("max_cost").desc())
)
