from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy("shipping_fee", "id")
w_recent = Window.partitionBy(F.year("order_date"), "status").orderBy(F.col("discount").desc_nulls_first())
w_grp = Window.orderBy("row_num", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("discount_cume_dist", "id")
w6 = Window.partitionBy("customer_id", "store_id").orderBy(F.col("rank_in_group").desc(), "id")

deduped = (
    df_orders
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        "row_num",
        F.lag("amount", 1).over(w2).alias("prev_amount"),
        F.cume_dist().over(w_recent).alias("discount_cume_dist"),
        F.avg("discount").over(w_grp).alias("discount_avg_6"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

deduped_orders = (
    latest
    .withColumn("rank_in_group_2", F.row_number().over(w6))
    .filter(F.col("rank_in_group_2") == 1)
)

eligible = (
    deduped_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.hour(F.col("c.paid_at")).between(12, 15)), "left_anti")
)

result = (
    eligible
    .groupBy(
        "store_id",
        F.when(F.col("prev_amount") >= 1460, "HIGH").otherwise("LOW").alias("prev_amount_tier"),
    )
    .agg(
        F.percentile_approx("shipping_fee", 0.95).alias("shipping_fee_p95"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
    )
    .orderBy("shipping_fee_p95")
)
