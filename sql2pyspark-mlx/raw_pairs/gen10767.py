from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_shipments
    .filter((~F.col("carrier").eqNullSafe(F.lit("Kerry"))) & F.col("order_id").isNull())
    .select(
        "*",
        F.when(F.col("status").isin(["IN_TRANSIT", "LOST"]), "Core").otherwise("Other").alias("status_group"),
    )
)

joined = (
    valid
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id").eqNullSafe(F.col("p.id")), "left")
    .select("e.*", F.col("p.status").alias("order_status"))
)

enriched = (
    joined
    .withColumn("last3_avg_cost", F.avg("cost").over(w_running))
    .withColumn("last3_sum_cost", F.sum("cost").over(w_running))
    .withColumn("last3_max_cost", F.max("cost").over(w_running))
)

filtered = (
    enriched
    .filter((F.col("shipped_date") >= F.lit("2023-04-28").cast("date")) & (F.col("carrier").eqNullSafe(F.lit("DHL"))))
    .select(
        "*",
        (F.col("last3_sum_cost") + F.col("cost")).alias("last3_sum_cost_plus_cost"),
        F.coalesce("order_status", F.lit("DIRECT")).alias("order_status_filled"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("cost") >= 77, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.avg("last3_sum_cost_plus_cost").alias("avg_last3_sum_cost_plus_cost"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.count_if(F.col("order_status") == "CANCELLED").alias("cancelled_events"),
    )
    .orderBy(F.col("avg_last3_sum_cost_plus_cost").desc())
)
