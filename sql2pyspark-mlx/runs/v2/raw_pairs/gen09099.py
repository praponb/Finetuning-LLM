from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("status").isin(["DELIVERED", "LOST"]))
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "DHL", 1).otherwise(0))) / F.count("*")), 1).alias("pct_dhl"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("total_cost") < 150)
    .drop("total_cost")
    .orderBy(F.col("order_id").desc())
)
