from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w2 = Window.partitionBy("status", "carrier").orderBy(F.col("shipped_date").desc(), "id")
w3 = Window.orderBy("shipping_fee", "id")
w4 = Window.partitionBy(F.month("order_date"), "status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w6 = Window.partitionBy("order_id", "carrier").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("order_id", "status").orderBy("shipped_date", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy(F.date_trunc("MONTH", "order_date")).orderBy("running_amount", "id").rowsBetween(-5, -1)
w9 = Window.partitionBy("carrier").orderBy(F.col("shipping_fee").desc(), "id")
w10 = Window.partitionBy(F.year("order_date"))
w11 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_shipments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    latest
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.shipping_fee").between(25, 37))
    .select("s.*", F.col("p.discount"))
)

enriched = (
    with_dims
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.shipped_date",
        "src.cost",
        "src.status",
        "src.dedup_rank",
        F.col("b.amount"),
        F.col("b.order_date"),
        F.col("b.shipping_fee"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked
    .withColumn("shipping_fee_bucket", F.ntile(10).over(w3))
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w4))
    .withColumn("cost_bucket", F.ntile(5).over(w5))
    .withColumn("running_amount", F.sum("amount").over(w6))
)

scored = (
    with_windows
    .select(
        "id",
        "status",
        "dedup_rank",
        "amount",
        "shipping_fee",
        "rn",
        "shipping_fee_bucket",
        "first_known_amount",
        F.avg("shipping_fee_bucket").over(w_grp).alias("shipping_fee_bucket_avg_5_prior"),
        F.avg("cost").over(w8).alias("cost_avg_5_prior"),
        F.row_number().over(w9).alias("seq_no"),
        F.round((F.col("cost_bucket") / F.sum("cost_bucket").over(w10)), 4).alias("cost_bucket_share"),
        F.sum("shipping_fee_bucket").over(w11).alias("cumulative_shipping_fee_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "status",
        "dedup_rank",
        "amount",
        "shipping_fee",
        "rn",
        "first_known_amount",
        "cost_avg_5_prior",
        "seq_no",
    )
)
