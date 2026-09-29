from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("discount", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("order_id").orderBy("years_since_paid_at", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("discount").asc_nulls_last())
w4 = Window.partitionBy("channel").orderBy("years_since_paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy(F.col("discount").desc(), "id")

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.to_date(F.lit("2023-06-04"))) & ~F.col("method").isin(["cash", "promptpay"]))
    .select(
        "id",
        "order_id",
        "amount",
        (F.datediff(F.current_date(), "paid_at") / 365.25).alias("years_since_paid_at"),
    )
)

joined = (
    cleaned
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.status") != "PENDING")
    .select(
        "a.id",
        "a.order_id",
        "a.years_since_paid_at",
        F.col("p.discount"),
        F.col("p.status"),
        F.col("p.channel"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "discount",
        "status",
        "channel",
        F.avg("discount").over(w).alias("discount_avg_6_prior"),
        F.lead("discount", 1).over(w2).alias("next_discount"),
        F.rank().over(w3).alias("discount_rank"),
        F.sum("years_since_paid_at").over(w4).alias("cumulative_years_since_paid_at"),
        F.ntile(10).over(w5).alias("discount_bucket"),
    )
)

base = (
    metrics
    .filter(F.col("status").isNotNull() & (F.col("discount_avg_6_prior") == 0))
    .withColumn("channel_status_key", F.concat_ws("/", "channel", "status"))
)

result = (
    base
    .select("id", "channel", "discount", "next_discount")
    .unpivot(
        ids=["id", "channel"],
        values=["discount", "next_discount"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
