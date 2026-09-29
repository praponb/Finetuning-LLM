from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_last(), "id")
w2 = Window.partitionBy("method").orderBy(F.col("latest_rank").desc(), "id")
w3 = Window.partitionBy("method").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("amount").desc())
w5 = Window.partitionBy("order_id").orderBy("amount_cume_dist", "id")
w6 = Window.partitionBy("order_id").orderBy("latest_rank", "id")

valid = (
    df_payments
    .filter(F.col("paid_at") >= "2023-05-23")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        F.when(F.col("status").isin(["REFUNDED", "FAILED"]), "Core").otherwise("Other").alias("status_group"),
        F.when(F.col("amount") >= 1240, "High").when(F.col("amount") >= 400, "Medium").otherwise("Low").alias("amount_tier"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

metrics = (
    ranked
    .filter(F.col("order_id").isNull())
    .select(
        "*",
        F.sum("amount").over(w3).alias("running_sum_amount"),
        F.max("amount").over(w3).alias("running_max_amount"),
        F.count("amount").over(w3).alias("running_count_amount"),
        F.min("amount").over(w3).alias("running_min_amount"),
        F.cume_dist().over(w4).alias("amount_cume_dist"),
    )
)

cleaned = (
    metrics
    .filter(F.col("method").isNotNull())
    .select(
        "*",
        F.when(F.col("running_max_amount") >= 1480, "High").when(F.col("running_max_amount") >= 210, "Medium").otherwise("Low").alias("running_max_amount_level"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

ranked_payments = (
    cleaned
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select(
        "id",
        "order_id",
        "amount",
        "latest_rank",
        "running_sum_amount",
        "running_count_amount",
        "amount_cume_dist",
        "running_max_amount_level",
        "resolved_method",
    )
)

enriched = (
    ranked_payments
    .select(
        "*",
        F.lead("running_count_amount", 2).over(w5).alias("next_running_count_amount"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("running_sum_amount") >= 6100, "PREMIUM").otherwise("STANDARD").alias("running_sum_amount_tier"),
        "order_id",
    )
    .agg(
        F.round(F.avg("amount_cume_dist"), 2).alias("avg_amount_cume_dist"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("avg_amount_cume_dist") < 1000000)
    .orderBy("avg_amount_cume_dist")
)
