from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy(F.col("paid_at").desc_nulls_last(), "id")
w_running = Window.partitionBy("order_id", "status")
w3 = Window.partitionBy("method", "status").orderBy(F.col("amount").desc_nulls_first())
w_recent = Window.partitionBy("status", "order_id").orderBy(F.col("amount").desc())
w_grp = Window.partitionBy("status", "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy("method")
w7 = Window.partitionBy(F.to_date("paid_at"))
w8 = Window.partitionBy(F.year("paid_at"))

latest = (
    df_payments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

matched_keys = (
    latest
    .select("id", "order_id")
    .intersect(
        df_payments
        .filter(F.col("order_id").isin([2, 24]))
        .select("id", "order_id")
    )
)

verified = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .filter(F.to_date("paid_at") < "2023-04-06")
    .select(
        "order_id",
        "amount",
        "paid_at",
        F.max("amount").over(w_running).alias("order_id_max_amount"),
        F.dense_rank().over(w3).alias("amount_dense_rank"),
        F.percent_rank().over(w_recent).alias("amount_percent_rank"),
        F.count("amount").over(w_grp).alias("amount_count_7d"),
        F.round((F.col("amount") / F.sum("amount").over(w6)), 4).alias("amount_share"),
    )
)

cleaned = (
    scored
    .filter((F.col("paid_at") >= F.lit("2023-10-25").cast("date")) & F.col("amount_count_7d").between(7, 12))
    .select(
        "*",
        F.when(F.col("amount_percent_rank") >= 0.79, "High").when(F.col("amount_percent_rank") >= 0.23, "Medium").otherwise("Low").alias("amount_percent_rank_level"),
    )
)

base = (
    cleaned
    .filter((F.col("paid_at") > F.lit("2023-07-26").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "paid_at",
        "order_id_max_amount",
        "amount_dense_rank",
        "amount_percent_rank",
        "amount_count_7d",
        "amount_share",
        "amount_percent_rank_level",
        (F.col("amount_share") + F.col("amount")).alias("amount_share_plus_amount"),
    )
)

valid = (
    base
    .filter((F.col("paid_at") >= F.lit("2023-08-13").cast("date")) & (F.col("amount_dense_rank") >= 8))
    .select(
        "*",
        F.round((F.col("amount_dense_rank") * 1.1), 2).alias("amount_dense_rank_scaled"),
    )
)

valid_payments = (
    valid
    .filter(F.col("paid_at") > F.lit("2023-06-27").cast("date"))
    .select(
        "paid_at",
        "order_id_max_amount",
        "amount_dense_rank",
        "amount_percent_rank",
        "amount_count_7d",
        "amount_share",
        "amount_percent_rank_level",
        "amount_share_plus_amount",
        "amount_dense_rank_scaled",
        (F.col("amount_share_plus_amount") - F.col("amount_share")).alias("amount_share_plus_amount_minus_amount_share"),
        (F.col("amount_dense_rank_scaled") * F.col("amount_percent_rank")).alias("amount_dense_rank_scaled_times_amount_percent_rank"),
    )
)

result = (
    valid_payments
    .select(
        "amount_dense_rank",
        "amount_share",
        "amount_share_plus_amount_minus_amount_share",
        "amount_dense_rank_scaled_times_amount_percent_rank",
        F.max("amount_dense_rank_scaled").over(w7).alias("paid_at_to_date_max_amount_dense_rank_scaled"),
        F.max("amount_count_7d").over(w8).alias("paid_at_year_max_amount_count_7d"),
    )
)
