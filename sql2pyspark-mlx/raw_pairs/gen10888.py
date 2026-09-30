from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("order_date")).orderBy(F.col("discount").desc_nulls_first())
w2 = Window.partitionBy("order_id", "order_status").orderBy("paid_at", "id").rowsBetween(-2, -1)
w_hist = Window.partitionBy("order_status", "order_id")
w_grp = Window.partitionBy("order_id")
w5 = Window.partitionBy("order_status").orderBy("paid_at", "id")
w6 = Window.partitionBy("order_status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.to_date("paid_at"), "order_status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_status").orderBy(F.col("discount_avg_2_prior").asc_nulls_first())
w9 = Window.partitionBy("order_id").orderBy("island_id", "id")
w10 = Window.partitionBy("order_status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

enriched = (
    df_payments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.paid_at",
        "e.status",
        F.col("o.discount"),
        F.col("o.status").alias("order_status"),
        F.col("o.channel"),
    )
)

enriched_payments = (
    enriched
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id").eqNullSafe(F.col("b.id")), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.paid_at",
        "e.discount",
        "e.order_status",
        F.col("b.order_date"),
    )
)

enriched_payments_2 = (
    enriched_payments
    .withColumn("discount_cume_dist", F.cume_dist().over(w))
    .withColumn("discount_avg_2_prior", F.avg("discount").over(w2))
    .withColumn("order_status_avg_discount", F.avg("discount").over(w_hist))
    .withColumn("order_id_avg_discount", F.avg("discount").over(w_grp))
)

flagged = (
    enriched_payments_2
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w5).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w5) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "order_id",
        "paid_at",
        "discount",
        "order_status",
        "order_date",
        "discount_avg_2_prior",
        F.sum("is_new_session").over(w6).alias("island_id"),
    )
)

enriched_payments_3 = (
    islands
    .withColumn("last_known_discount", F.last("discount", ignorenulls=True).over(w7))
    .withColumn("discount_avg_2_prior_rank", F.rank().over(w8))
    .withColumn("island_id_bucket", F.ntile(10).over(w9))
    .withColumn("discount_avg_2_prior_sum_7d", F.sum("discount_avg_2_prior").over(w10))
)

result = (
    enriched_payments_3
    .select(
        "id",
        "order_id",
        "paid_at",
        "discount",
        "order_status",
        "last_known_discount",
        "discount_avg_2_prior_rank",
        "island_id_bucket",
        "discount_avg_2_prior_sum_7d",
    )
    .orderBy("discount_avg_2_prior_sum_7d")
)
