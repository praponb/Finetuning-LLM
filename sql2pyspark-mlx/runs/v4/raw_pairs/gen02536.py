from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc_nulls_first())
w2 = Window.orderBy("paid_at", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy(F.col("amount").desc())
w4 = Window.partitionBy("method").orderBy("amount", "id")
w5 = Window.partitionBy("status").orderBy(F.col("amount").asc_nulls_first(), "id")

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2024-01-13 07:00:00").cast("timestamp")) & F.col("method").isNotNull() & ~F.col("method").isin(["card", "promptpay", "bank_transfer"]))
    .withColumn("clean_method", F.trim("method"))
)

enriched = (
    base
    .withColumn("amount_dense_rank", F.dense_rank().over(w))
    .withColumn("amount_avg_5", F.avg("amount").over(w2))
    .withColumn("amount_dense_rank_2", F.dense_rank().over(w3))
    .withColumn("amount_bucket", F.ntile(3).over(w4))
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("amount_avg_5") >= 1662, "LARGE").otherwise("SMALL").alias("amount_avg_5_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.min("amount_dense_rank").alias("min_amount_dense_rank"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("min_amount_dense_rank").asc_nulls_first())
)
