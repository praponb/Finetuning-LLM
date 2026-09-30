from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.date_trunc("MONTH", "delivered_date").alias("delivered_month"),
        F.when(F.col("carrier").isin(["UPS", "DHL"]), "group_a").otherwise("other").alias("carrier_group"),
    )
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
    )
)
