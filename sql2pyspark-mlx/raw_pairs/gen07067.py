from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "store_id").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("order_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("running_sum_amount", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("store_id").orderBy("running_count_amount", "id")
w5 = Window.partitionBy("status").orderBy("amount")
w6 = Window.partitionBy("status").orderBy("months_order_date_to_order_date_plus_months", "id")
w7 = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_last())

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-07-23"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.add_months("order_date", 12).alias("order_date_plus_months"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "order_date_plus_months",
    )
)

base = (
    latest
    .filter(F.col("customer_id").isNotNull() & F.col("order_date_plus_months").between("2023-07-01", "2024-07-01"))
    .withColumn("months_order_date_to_order_date_plus_months", F.round((F.datediff("order_date_plus_months", "order_date") / 30), 1))
)

scored = (
    base
    .select(
        "*",
        F.max("amount").over(w2).alias("running_max_amount"),
        F.count("amount").over(w2).alias("running_count_amount"),
        F.min("amount").over(w2).alias("running_min_amount"),
        F.sum("amount").over(w2).alias("running_sum_amount"),
    )
)

with_windows = (
    scored
    .filter(F.col("status") == "CANCELLED")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "order_date_plus_months",
        "months_order_date_to_order_date_plus_months",
        "running_max_amount",
        "running_min_amount",
        F.avg("running_count_amount").over(w3).alias("running_count_amount_avg_2_prior"),
        F.lag("running_sum_amount", 2).over(w4).alias("prev_running_sum_amount"),
        F.dense_rank().over(w5).alias("amount_dense_rank"),
        F.lead("running_sum_amount", 2).over(w6).alias("next_running_sum_amount"),
        F.rank().over(w7).alias("amount_rank"),
    )
)

result = (
    with_windows
    .withColumn("week_start", F.date_trunc("WEEK", "order_date_plus_months"))
    .withColumn("months_order_date_to_order_date_plus_months_tier", F.when(F.col("months_order_date_to_order_date_plus_months") >= 61, "LARGE").otherwise("SMALL"))
    .withColumn("amount_rank_tier", F.when(F.col("amount_rank") >= 7, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["week_start", "months_order_date_to_order_date_plus_months_tier", "amount_rank_tier"], ["week_start", "months_order_date_to_order_date_plus_months_tier"], ["week_start"], []],
        "week_start",
        "months_order_date_to_order_date_plus_months_tier",
        "amount_rank_tier",
    )
    .agg(
        F.round(F.sum("running_max_amount") / F.nullif(F.count(F.when(F.col("status") == "RETURNED", 1)), F.lit(0)), 2).alias("running_max_amount_per_returned"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
    )
)
