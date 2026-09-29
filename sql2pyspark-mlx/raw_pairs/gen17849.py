from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("amount", "id").rowsBetween(-6, -1)

metrics = (
    df_payments
    .withColumn("last7_max_amount", F.max("amount").over(w_hist))
    .withColumn("last7_count_amount", F.count("amount").over(w_hist))
    .withColumn("last7_min_amount", F.min("amount").over(w_hist))
    .withColumn("amount_avg_6_prior", F.avg("amount").over(w2))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("amount") >= 1330, "HIGH").otherwise("LOW").alias("amount_tier"),
        "order_id",
    )
    .agg(
        F.percentile_approx("last7_count_amount", 0.95).alias("last7_count_amount_p95"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
        F.round(F.avg("last7_count_amount"), 2).alias("avg_last7_count_amount"),
    )
    .orderBy("avg_last7_count_amount")
)
