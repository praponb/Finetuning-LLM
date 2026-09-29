from pyspark.sql import functions as F

result = (
    df_shipments
    .select("id", "order_id", F.round("cost", 0).alias("cost_rounded"))
    .orderBy(F.col("id").desc())
)
