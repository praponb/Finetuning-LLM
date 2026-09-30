from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id", "attr_key")
w2 = Window.partitionBy("customer_id").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy("island_id", "id", "attr_key")
w_recent = Window.partitionBy("attr_key", "customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_part = Window.partitionBy("customer_id").orderBy("prev_island_id_bucket", "id", "attr_key")
w7 = Window.partitionBy("attr_key", "customer_id")
w_ordered = Window.partitionBy("attr_key").orderBy("island_id_bucket", "id", "attr_key")
w9 = Window.partitionBy("customer_id").orderBy(F.col("next_island_id_sum_30m").desc_nulls_first())
w10 = Window.partitionBy("customer_id").orderBy("island_id_bucket", "id", "attr_key").rowsBetween(-6, -1)
w11 = Window.partitionBy("customer_id").orderBy("event_seq_avg_6_prior", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("customer_id", "attr_key").orderBy(F.col("event_seq_avg_6_prior").desc(), "id", "attr_key")
w13 = Window.partitionBy("customer_id").orderBy("event_seq_2", "id", "attr_key")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

flagged = (
    flattened
    .withColumn("session_start_flag", F.when(F.col("created_at") - F.lag("created_at").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "attr_key",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

enriched = (
    sessionized
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "attr_key",
        F.lead("island_id", 1).over(w3).alias("next_island_id"),
        F.ntile(10).over(w4).alias("island_id_bucket"),
        F.sum("island_id").over(w_recent).alias("island_id_sum_30m"),
    )
)

scored = (
    enriched
    .filter(F.col("attr_key").isin(["utm_source", "gift"]))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attr_key",
        "next_island_id",
        "island_id_bucket",
        F.lead("island_id_sum_30m").over(w).alias("next_island_id_sum_30m"),
        F.lag("island_id_bucket").over(w).alias("prev_island_id_bucket"),
        F.row_number().over(w).alias("event_seq"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.row_number().over(w_part).alias("event_seq_2"),
        F.sum("island_id_bucket").over(w7).alias("attr_key_sum_island_id_bucket"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.cume_dist().over(w9).alias("next_island_id_sum_30m_cume_dist"),
        F.avg("event_seq").over(w10).alias("event_seq_avg_6_prior"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "customer_id",
        "attr_key",
        "next_island_id_sum_30m",
        "prev_island_id_bucket",
        "event_seq_2",
        "attr_key_sum_island_id_bucket",
        "next_island_id_sum_30m_cume_dist",
        "event_seq_avg_6_prior",
        F.sum("event_seq_avg_6_prior").over(w11).alias("running_event_seq_avg_6_prior"),
        F.row_number().over(w12).alias("seq_no_2"),
        F.row_number().over(w13).alias("event_seq_3"),
    )
)

result = (
    with_windows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "attr_key",
        "prev_island_id_bucket",
        "attr_key_sum_island_id_bucket",
        "next_island_id_sum_30m_cume_dist",
        "event_seq_avg_6_prior",
        "seq_no_2",
        F.when(F.col("attr_key").rlike("^(device|utm_source)$"), "CORE").when(F.col("attr_key").rlike("^(gift|weight_kg)$"), "PRIORITY").otherwise("GROUP_B").alias("attr_key_zone"),
        F.col("min_account_balance"),
    )
    .orderBy(F.col("min_account_balance").asc_nulls_last())
)
