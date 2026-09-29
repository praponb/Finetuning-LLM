from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("status").orderBy("opened_date", "id")
w5 = Window.partitionBy("channel", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy(F.col("sqft").asc_nulls_last(), "id")
w7 = Window.partitionBy("region").orderBy("prev_sqft", "id")

not_flagged = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.units") > 20), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.channel"), F.col("c.status"))
)

with_windows = (
    latest_match
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        "channel",
        F.last("region", ignorenulls=True).over(w2).alias("last_known_region"),
        F.last("channel", ignorenulls=True).over(w3).alias("last_known_channel"),
        F.lag("sqft", 1, 0).over(w_running).alias("prev_sqft"),
        F.sum("sqft").over(w5).alias("running_sqft"),
    )
)

ranked = (
    with_windows
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    ranked
    .withColumn("next_sqft", F.lead("sqft").over(w7))
    .withColumn("prev_sqft_2", F.lag("sqft").over(w7))
    .withColumn("next_prev_sqft", F.lead("prev_sqft").over(w7))
    .withColumn("prev_prev_sqft", F.lag("prev_sqft").over(w7))
    .withColumn("seq_no", F.row_number().over(w7))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("prev_sqft") >= 16675, "HIGH").otherwise("LOW").alias("prev_sqft_tier"),
        F.when(F.col("next_prev_sqft") >= 12307, "PREMIUM").otherwise("STANDARD").alias("next_prev_sqft_tier"),
    )
    .agg(
        F.sum("prev_sqft_2").alias("total_prev_sqft_2"),
        F.sum(F.when(F.col("channel") == "partner", F.col("seq_no")).otherwise(0)).alias("partner_seq_no"),
    )
)
