from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("plan").orderBy(F.col("credit_limit").desc_nulls_first(), "id")
w3 = Window.orderBy("credit_limit", "id")
w4 = Window.partitionBy("country").orderBy("credit_limit", "id")
w5 = Window.partitionBy("country").orderBy(F.col("prev_credit_limit").asc_nulls_last(), "id")
w6 = Window.partitionBy("country").orderBy(F.col("prev_credit_limit").desc(), "id")
w7 = Window.partitionBy("country").orderBy(F.col("row_seq").asc_nulls_first(), "id")

enriched = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.plan",
        "s.monthly_fee",
        "s.status",
        F.col("p.country"),
        F.col("p.city"),
        F.col("p.credit_limit"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "country", "credit_limit")
)

enriched_subscriptions = (
    ranked
    .select(
        "*",
        F.lag("credit_limit", 1).over(w3).alias("prev_credit_limit"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

latest_subscriptions = (
    enriched_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    latest_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

deduped_subscriptions = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
    .select("id", "prev_credit_limit", "row_seq", "dedup_rank")
)

result = (
    deduped_subscriptions
    .groupBy(
        F.when(F.col("row_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
    )
    .agg(
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.percentile_approx("prev_credit_limit", 0.9).alias("prev_credit_limit_p90"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq_2"),
    )
    .orderBy(F.col("avg_row_seq_2").desc())
)
