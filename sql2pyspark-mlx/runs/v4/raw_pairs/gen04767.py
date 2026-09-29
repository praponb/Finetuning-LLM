from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("carrier")
w3 = Window.partitionBy("order_id").orderBy(F.col("shipped_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("order_status").orderBy(F.col("cost_share").desc())
w5 = Window.partitionBy("order_id").orderBy("cost_max_90d")
w6 = Window.partitionBy("carrier").orderBy(F.col("cost").desc())

base = (
    df_shipments
    .filter(F.col("shipped_date") >= F.lit("2024-01-04").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.date_trunc("year", "shipped_date").alias("shipped_date_period"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled"),
    )
)

matched_keys = (
    base
    .select("carrier")
    .intersect(
        df_shipments
        .filter(F.col("cost") < 79)
        .select("carrier")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.status").alias("order_status"), F.col("b.amount"))
)

metrics = (
    with_dims
    .withColumn("cost_max_90d", F.max("cost").over(w))
    .withColumn("carrier_stddev_samp_amount", F.stddev_samp("amount").over(w2))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w2)), 4))
    .withColumn("cost_max_7d", F.max("cost").over(w3))
)

result = (
    metrics
    .select(
        "*",
        F.rank().over(w4).alias("cost_share_rank"),
        F.rank().over(w5).alias("cost_max_90d_rank"),
        F.percent_rank().over(w6).alias("cost_percent_rank"),
    )
)
