from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") <= "2025-02-08") & F.col("amount").between(1100, 1160))
    .groupBy(
        F.when(F.col("status").isin(["CANCELLED", "RETURNED"]), "tier_1").otherwise("other").alias("status_group"),
        F.date_format("order_date", "yyyy-MM").alias("order_month"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .orderBy("order_month")
)
