from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "web"]) & F.col("status").isin(["CANCELLED", "RETURNED", "SHIPPED"]) & F.col("amount").isNotNull())
    .agg(
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("status_values")
)
