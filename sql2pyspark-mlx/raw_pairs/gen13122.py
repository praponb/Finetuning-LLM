from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 39)
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.sum(F.when(F.col("channel") == "web", F.col("discount")).otherwise(0)).alias("web_discount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("web_discount") > 71)
    .orderBy("web_discount")
)
