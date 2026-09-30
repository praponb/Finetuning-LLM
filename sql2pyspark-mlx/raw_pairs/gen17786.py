from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_status", "order_id").orderBy(F.col("cost").asc_nulls_last(), "id")
w2 = Window.partitionBy("order_status", "order_id").orderBy(F.col("discount").desc(), "id")
w3 = Window.partitionBy("carrier").orderBy("cost", "id")
w4 = Window.partitionBy("order_status", "order_id").orderBy(F.col("cost").desc(), "id")
w5 = Window.partitionBy("carrier").orderBy(F.col("cost").desc())
w6 = Window.partitionBy(F.date_trunc("YEAR", "delivered_date"), "order_status")
w7 = Window.partitionBy("order_id").orderBy(F.col("cost_rank").desc(), "id")
w8 = Window.partitionBy("order_id").orderBy("cost_bucket_2", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.partitionBy("order_status").orderBy(F.col("cost_bucket_2").desc_nulls_first())
w10 = Window.orderBy(F.col("cost_rank").asc_nulls_last())

with_dims = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.discount"))
)

with_dims_shipments = (
    with_dims
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.status").alias("order_status"))
)

ranked = (
    with_dims_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

ranked_shipments = (
    ranked
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "order_status",
    )
)

scored = (
    ranked_shipments
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "order_status",
        F.ntile(5).over(w3).alias("cost_bucket"),
        F.ntile(10).over(w4).alias("cost_bucket_2"),
        F.rank().over(w5).alias("cost_rank"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "cost",
        "order_status",
        "cost_bucket",
        "cost_bucket_2",
        "cost_rank",
        F.round((F.col("cost_bucket_2") / F.sum("cost_bucket_2").over(w6)), 4).alias("cost_bucket_2_share"),
        F.ntile(10).over(w7).alias("cost_rank_bucket"),
        F.avg("cost").over(w8).alias("cost_avg_5"),
    )
)

metrics = (
    enriched
    .filter(F.col("cost_avg_5") != 94)
    .select(
        "*",
        F.dense_rank().over(w9).alias("cost_bucket_2_dense_rank"),
        F.percent_rank().over(w10).alias("cost_rank_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "cost",
        "cost_bucket",
        "cost_bucket_2",
        "cost_rank",
        "cost_rank_bucket",
        "cost_bucket_2_dense_rank",
        "cost_rank_percent_rank",
    )
)
