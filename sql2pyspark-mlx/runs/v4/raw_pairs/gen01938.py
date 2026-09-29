from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("starts_on", "id")
w3 = Window.partitionBy("channel")
w4 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("channel").orderBy("ends_on", "id")
w6 = Window.orderBy(F.col("seq_no").asc_nulls_last())
w7 = Window.partitionBy("channel").orderBy(F.col("seq_no").desc(), "id")
w8 = Window.orderBy("prev_seq_no", "id")

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull() & F.col("promo_code").isin(["BUNDLE", "VIP", "WELCOME5"]))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        F.when(F.col("discount_pct") >= 0.33, "High").when(F.col("discount_pct") >= 0.16, "Medium").otherwise("Low").alias("discount_pct_tier"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("seq_no_2"),
    )
)

with_windows = (
    scored
    .withColumn("channel_max_seq_no_2", F.max("seq_no_2").over(w3))
    .withColumn("seq_no_2_avg_2_prior", F.avg("seq_no_2").over(w4))
    .withColumn("prev_seq_no", F.lag("seq_no", 1).over(w5))
    .withColumn("seq_no_rank", F.rank().over(w6))
)

metrics = (
    with_windows
    .select(
        "*",
        F.ntile(4).over(w7).alias("seq_no_bucket"),
        F.row_number().over(w8).alias("row_seq"),
        F.max("seq_no").over(w3).alias("channel_max_seq_no"),
        F.round((F.col("prev_seq_no") / F.sum("prev_seq_no").over(w3)), 4).alias("prev_seq_no_share"),
    )
)

result = (
    metrics
    .withColumn("seq_no_2_tier", F.when(F.col("seq_no_2") >= 9, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["channel", "seq_no_2_tier"], ["channel"], []],
        "channel",
        "seq_no_2_tier",
    )
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.grouping("channel").alias("gid"),
    )
    .filter(F.col("num_channels") < 1)
    .orderBy(F.col("channel").desc_nulls_first())
)
