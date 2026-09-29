from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter((F.col("method") != "cash") & F.hour("paid_at").between(10, 17))
    .withColumn("status_prefix", F.substring("status", 1, 4))
)

agg = (
    filtered_payments
    .groupBy(
        F.when(F.col("amount") >= 1260, "high").when(F.col("amount") >= 380, "mid").otherwise("low").alias("amount_bucket"),
        "method",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
)

result = (
    agg
    .select("amount_bucket", "method")
    .orderBy("method", "amount_bucket")
    .limit(3)
)
