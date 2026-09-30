from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_first(), "id")
w2 = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w3 = Window.orderBy(F.col("cost").desc_nulls_first())
w4 = Window.partitionBy("channel")
w5 = Window.partitionBy(F.year("delivered_date"), "status")

filtered = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .withColumn("weeks_delivered_date_to_shipped_date", (F.datediff("shipped_date", "delivered_date") / 7))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("N/A")))
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_dims = (
    ranked
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.shipping_fee").between(15, 30))
    .select(
        "src.*",
        F.col("p.order_date"),
        F.col("p.channel"),
        F.col("p.status").alias("order_status"),
    )
)

enriched = (
    with_dims
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "status",
        "resolved_carrier",
        "channel",
        "order_status",
        F.lead("weeks_delivered_date_to_shipped_date", 1).over(w2).alias("next_weeks_delivered_date_to_shipped_date"),
        F.dense_rank().over(w3).alias("cost_dense_rank"),
    )
)

enriched_shipments = (
    enriched
    .withColumn("cost_dense_rank_share", F.round((F.col("cost_dense_rank") / F.sum("cost_dense_rank").over(w4)), 4))
    .withColumn("delivered_date_year_sum_next_weeks_delivered_date_to_shipped_date", F.sum("next_weeks_delivered_date_to_shipped_date").over(w5))
)

stats = (
    enriched_shipments
    .groupBy(
        F.date_trunc("YEAR", "delivered_date").alias("year_start"),
        F.date_trunc("QUARTER", "delivered_date").alias("quarter_start"),
    )
    .agg(
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
    )
)

result = (
    stats
    .select("year_start", "quarter_start", "delivered_count", "row_count", "web_count")
    .orderBy(F.col("quarter_start").desc(), F.col("year_start").desc())
)
