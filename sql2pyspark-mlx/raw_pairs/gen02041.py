from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("start_date").desc_nulls_first(), "id")
w_part = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id")
w_recent = Window.partitionBy("status").orderBy("start_date", "id")
w_ordered = Window.partitionBy(F.month("start_date"), "status").orderBy(F.col("start_date").desc(), "id")
w5 = Window.partitionBy(F.month("start_date"))
w_running = Window.orderBy("row_seq", "id")
w7 = Window.orderBy(F.col("row_seq_share").desc(), "id")
w8 = Window.orderBy(F.col("next_seq_no").desc(), "id")
w9 = Window.orderBy("next_seq_no_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("row_seq_share", "id")
w11 = Window.orderBy(F.col("seq_no_2").desc_nulls_last())
w12 = Window.orderBy("row_seq_share_bucket", "id")

enriched = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.plan",
        "src.start_date",
        "src.status",
        F.col("p.country"),
        F.col("p.name"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "start_date", "status", "name")
)

enriched_subscriptions = (
    latest
    .filter(~F.col("name").like("%Rossi%"))
    .select(
        "*",
        F.row_number().over(w_part).alias("row_seq"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.row_number().over(w_part).alias("event_seq_2"),
    )
)

enriched_subscriptions_2 = (
    enriched_subscriptions
    .select(
        "id",
        "name",
        "row_seq",
        "event_seq",
        "event_seq_2",
        F.lag("seq_no").over(w_recent).alias("prev_seq_no"),
        F.lead("seq_no").over(w_recent).alias("next_seq_no"),
        F.lead("row_seq").over(w_recent).alias("next_row_seq"),
        F.lag("row_seq").over(w_recent).alias("prev_row_seq"),
        F.row_number().over(w_recent).alias("seq_no_2"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w5)), 4).alias("row_seq_share"),
    )
)

valid = (
    enriched_subscriptions_2
    .filter(F.col("row_seq").isNotNull())
    .select("*", F.round("next_seq_no", 1).alias("next_seq_no_rounded"))
)

with_windows = (
    valid
    .select(
        "*",
        F.ntile(5).over(w_running).alias("row_seq_bucket"),
        F.ntile(3).over(w7).alias("row_seq_share_bucket"),
        F.row_number().over(w8).alias("seq_no"),
        F.last("row_seq", ignorenulls=True).over(w9).alias("last_known_row_seq"),
    )
)

with_windows_subscriptions = (
    with_windows
    .select(
        "*",
        F.ntile(3).over(w10).alias("row_seq_share_bucket_2"),
        F.rank().over(w11).alias("seq_no_2_rank"),
        F.lag("row_seq_bucket", 1).over(w12).alias("prev_row_seq_bucket"),
    )
)

result = (
    with_windows_subscriptions
    .groupBy(
        F.when(F.col("prev_seq_no") >= 8, "PREMIUM").otherwise("STANDARD").alias("prev_seq_no_tier"),
        "prev_seq_no",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("row_seq_share_bucket", 0.5).alias("row_seq_share_bucket_p50"),
    )
    .orderBy(F.col("prev_seq_no").desc())
)
