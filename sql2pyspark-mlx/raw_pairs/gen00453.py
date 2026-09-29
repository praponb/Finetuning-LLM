from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("order_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("amount").asc_nulls_first(), "id")
w_part = Window.partitionBy("status").orderBy("latest_rank", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy(F.col("discount").desc_nulls_last())
w5 = Window.orderBy(F.col("discount").asc_nulls_last())
w_hist = Window.partitionBy("channel")
w7 = Window.partitionBy("customer_id")
w8 = Window.partitionBy("channel").orderBy(F.col("last7_min_discount").desc_nulls_last())
w9 = Window.orderBy("amount")
w10 = Window.partitionBy("channel").orderBy(F.col("last7_min_discount").desc())

latest = (
    df_orders
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "latest_rank",
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.min("discount").over(w_part).alias("last7_min_discount"),
        F.sum("discount").over(w_part).alias("last7_sum_discount"),
        F.count("discount").over(w_part).alias("last7_count_discount"),
        F.avg("discount").over(w_part).alias("last7_avg_discount"),
        F.rank().over(w4).alias("discount_rank"),
    )
)

with_windows_orders = (
    with_windows
    .filter(F.col("channel") == "partner")
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "latest_rank",
        "last7_min_discount",
        "last7_count_discount",
        "discount_rank",
        F.rank().over(w5).alias("discount_rank_2"),
        F.min("last7_sum_discount").over(w_hist).alias("channel_min_last7_sum_discount"),
        F.max("last7_min_discount").over(w7).alias("customer_id_max_last7_min_discount"),
        F.rank().over(w8).alias("last7_min_discount_rank"),
        F.percent_rank().over(w9).alias("amount_percent_rank"),
    )
)

ranked_orders = (
    with_windows_orders
    .withColumn("latest_rank_2", F.rank().over(w10))
    .filter(F.col("latest_rank_2") == 1)
    .drop("latest_rank_2")
)

enriched = (
    ranked_orders
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("s.*", F.col("p.name"), F.col("p.signup_date"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.channel",
        "e.amount",
        "e.discount",
        "e.channel_min_last7_sum_discount",
        "e.signup_date",
        F.col("c.age"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "channel",
        F.col("discount").alias("DISCOUNT"),
        F.col("amount").alias("AMOUNT_VAL"),
    )
    .unpivot(
        ids=["id", "channel"],
        values=["DISCOUNT", "AMOUNT_VAL"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
