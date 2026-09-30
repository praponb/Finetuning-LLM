from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter(F.col("status") != "SUCCESS")
    .withColumn("method_group", F.when(F.col("method").isin(["cash", "paypal"]), "Tier 1").otherwise("Other"))
)

agg = (
    filtered_payments
    .groupBy("status", F.month("paid_at").alias("paid_month"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("total_count"),
    )
)

result = agg.orderBy("total_count", "status")
