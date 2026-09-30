from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")

ranked = (
    df_shipments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    ranked
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.status").alias("order_status"), F.col("d.channel"))
)

cleaned = (
    enriched
    .filter((F.col("shipped_date") > F.lit("2023-09-13").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
)

result = (
    cleaned
    .groupBy("channel")
    .agg(
        F.count(F.when(F.col("order_status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.min("cost_filled").alias("min_cost_filled"),
    )
    .orderBy(F.col("min_cost_filled").desc_nulls_last())
)
