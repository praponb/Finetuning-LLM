from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee")
w3 = Window.partitionBy("customer_id").orderBy("rn", "id")
w4 = Window.partitionBy("status", "customer_id").orderBy("monthly_fee")

deduped = (
    df_subscriptions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

deduped_subscriptions = (
    deduped
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    deduped_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "rn",
        F.lead("monthly_fee").over(w3).alias("next_monthly_fee"),
        F.lag("monthly_fee").over(w3).alias("prev_monthly_fee"),
        F.row_number().over(w3).alias("event_seq"),
        F.cume_dist().over(w4).alias("monthly_fee_cume_dist"),
    )
)

enriched_subscriptions = (
    enriched
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.segment") == "Corporate")
    .select("s.*", F.col("d.name"), F.col("d.segment"), F.col("d.credit_limit"))
)

stats = (
    enriched_subscriptions
    .rollup(
        F.when(F.col("credit_limit") >= 17900, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count_if(F.col("segment") == "SMB").alias("smb_events"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_rows"),
    )
)

result = (
    stats
    .select("credit_limit_tier", "segment_list", "num_segments", "consumer_rows")
    .orderBy(F.col("credit_limit_tier").desc_nulls_last())
)
