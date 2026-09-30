from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & ~F.col("method").isin(["card", "paypal", "promptpay"]))
    .withColumn("status_title", F.initcap(F.trim("status")))
)

order_id_summary = (
    base
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
)

result = (
    order_id_summary
    .select("order_id", "method_values")
    .orderBy(F.col("order_id").desc())
)
