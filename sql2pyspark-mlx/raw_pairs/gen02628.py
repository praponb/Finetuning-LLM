from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("region").orderBy("sqft", "id")
w3 = Window.orderBy(F.col("sqft").desc(), "id")
w4 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city", "region").orderBy(F.col("sqft_bucket_2").desc(), "id")
w6 = Window.orderBy("sqft_bucket_2", "id").rowsBetween(-5, -1)
w7 = Window.orderBy("sqft_bucket_2", "id")

valid = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-05-25").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "store_name", "region").alias("store_name_region_key"),
    )
)

metrics = (
    valid
    .withColumn("sqft_avg_5_prior", F.avg("sqft").over(w))
    .withColumn("sqft_bucket", F.ntile(4).over(w2))
    .withColumn("sqft_bucket_2", F.ntile(5).over(w3))
    .withColumn("last_known_region", F.last("region", ignorenulls=True).over(w4))
)

deduped = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_name", "sqft_bucket", "sqft_bucket_2")
)

enriched = (
    deduped
    .select(
        "id",
        "sqft_bucket",
        F.avg("sqft_bucket_2").over(w6).alias("sqft_bucket_2_avg_5_prior"),
        F.lag("sqft_bucket", 1).over(w7).alias("prev_sqft_bucket"),
    )
)

stats = (
    enriched
    .groupBy(
        F.when(F.col("sqft_bucket") >= 9, "HIGH").otherwise("LOW").alias("sqft_bucket_tier"),
    )
    .agg(
        F.round(F.avg("sqft_bucket_2_avg_5_prior"), 2).alias("avg_sqft_bucket_2_avg_5_prior"),
        F.round(F.avg("prev_sqft_bucket"), 2).alias("avg_prev_sqft_bucket"),
        F.round(F.avg("sqft_bucket_2_avg_5_prior"), 2).alias("avg_sqft_bucket_2_avg_5_prior_2"),
    )
    .filter(F.col("avg_sqft_bucket_2_avg_5_prior") > 0)
)

result = (
    stats
    .select("sqft_bucket_tier", "avg_sqft_bucket_2_avg_5_prior", "avg_prev_sqft_bucket")
    .orderBy(
        F.col("avg_prev_sqft_bucket").desc_nulls_first(),
        "avg_sqft_bucket_2_avg_5_prior",
    )
)
