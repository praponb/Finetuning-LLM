from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "IN_TRANSIT", "DELIVERED"]))
    .withColumn("days_shipped_date_to_delivered_date", F.datediff("delivered_date", "shipped_date"))
)

summary = (
    recent_shipments
    .groupBy(
        F.when(F.col("carrier").isin(["Flash", "Kerry"]), "tier_1").otherwise("other").alias("carrier_group"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
        F.sum("cost").alias("total_cost"),
    )
)

result = summary
