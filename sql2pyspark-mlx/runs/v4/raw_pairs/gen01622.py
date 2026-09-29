from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region", "city").orderBy("opened_date", "id")
w3 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("region").orderBy(F.col("next_sqft").desc(), "id")
w_ordered = Window.orderBy("sqft", "id")
w_recent = Window.orderBy(F.col("sqft_avg_2_prior").desc_nulls_first())
w8 = Window.orderBy(F.col("sqft_avg_2_prior").desc_nulls_last())
w9 = Window.partitionBy("city").orderBy(F.col("next_sqft").desc(), "id")
w10 = Window.partitionBy("city")

not_flagged = (
    df_stores
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id").isNull()), "left_semi")
)

with_windows = (
    not_flagged
    .select(
        "*",
        F.last("region", ignorenulls=True).over(w).alias("last_known_region"),
        F.lead("sqft", 1, 0).over(w2).alias("next_sqft"),
        F.ntile(10).over(w3).alias("sqft_bucket"),
        F.avg("sqft").over(w4).alias("sqft_avg_2_prior"),
    )
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    deduped
    .withColumn("row_seq", F.row_number().over(w_ordered))
    .withColumn("sqft_avg_2_prior_percent_rank", F.percent_rank().over(w_recent))
    .withColumn("sqft_avg_2_prior_cume_dist", F.cume_dist().over(w8))
    .withColumn("next_sqft_bucket", F.ntile(5).over(w9))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w10)), 4))
)

grouped = (
    enriched
    .groupBy(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.when(F.col("sqft") >= 16939, "HIGH").otherwise("LOW").alias("sqft_tier"),
    )
    .agg(
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
        F.bit_or("sqft").alias("sqft_bit_or"),
    )
    .filter(F.col("sqft_bit_or") < 1)
)

result = (
    grouped
    .select("quarter_start", "sqft_tier")
    .orderBy(F.col("quarter_start").desc(), F.col("sqft_tier").desc())
)
