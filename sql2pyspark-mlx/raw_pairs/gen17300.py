from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id")
w2 = Window.partitionBy(F.date_trunc("MONTH", "shipped_date"), "order_id")
w3 = Window.partitionBy(F.year("shipped_date"), "order_id")
w4 = Window.partitionBy("order_id", "carrier").orderBy(F.col("cost").desc_nulls_first())
w5 = Window.partitionBy("order_id", "status").orderBy(F.col("cost_share_2").desc(), "id")

metrics = (
    df_shipments
    .filter(F.col("order_id").isin([11, 16, 21, 34]))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.ntile(10).over(w).alias("cost_bucket"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.round((F.col("cost") / F.sum("cost").over(w3)), 4).alias("cost_share_2"),
        F.cume_dist().over(w4).alias("cost_cume_dist"),
    )
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "cost_bucket",
        "cost_share",
        "cost_cume_dist",
    )
)

enriched = (
    ranked
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.delivered_date",
        "src.cost",
        "src.cost_bucket",
        "src.cost_cume_dist",
        F.col("p.status"),
    )
)

matched_keys = (
    enriched
    .select("status", "id")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date") >= "2024-03-26")
        .select("status", "id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("delivered_date") >= F.lit("2023-04-14").cast("date"))
    .withColumn("cost_bucket_num", F.col("cost_bucket").cast("double"))
)

result = (
    filtered
    .groupBy("order_id")
    .pivot("status", ["PENDING", "COMPLETED", "CANCELLED"])
    .agg(F.sum("cost_cume_dist").alias("amount"), F.count(F.lit(1)).alias("n"))
)
