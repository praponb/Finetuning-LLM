from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("status_group", F.when(F.col("status").isin(["LOST", "IN_TRANSIT"]), "Tier 1").otherwise("Other"))
    .orderBy("order_id")
)
