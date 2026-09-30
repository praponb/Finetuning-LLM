from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy(F.year("opened_date"), "region").orderBy("opened_date", "id")
w3 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w5 = Window.partitionBy("region").orderBy(F.col("event_seq").desc_nulls_last(), "id")
w6 = Window.partitionBy("city").orderBy("last_known_is_east", "id")
w7 = Window.orderBy(F.col("last_known_is_east").asc_nulls_last())
w8 = Window.orderBy("prev_is_east", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.orderBy("prev_is_east", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        F.when(F.col("region") == "East", 1).otherwise(0).alias("is_east"),
        F.coalesce("region", F.lit("OTHER")).alias("resolved_region"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.avg("is_east").over(w).alias("is_east_avg_3"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("is_east", ignorenulls=True).over(w3).alias("last_known_is_east"),
        F.ntile(5).over(w4).alias("sqft_bucket"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "store_name",
        "city",
        "is_east",
        "resolved_region",
        "event_seq",
        "last_known_is_east",
        "sqft_bucket",
    )
)

metrics_stores = (
    latest
    .filter(F.col("last_known_is_east").between(62, 63))
    .select(
        "id",
        "resolved_region",
        "last_known_is_east",
        "sqft_bucket",
        F.lag("is_east").over(w6).alias("prev_is_east"),
        F.lag("sqft_bucket").over(w6).alias("prev_sqft_bucket"),
        F.lead("sqft_bucket").over(w6).alias("next_sqft_bucket"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

enriched = (
    metrics_stores
    .select(
        "*",
        F.cume_dist().over(w7).alias("last_known_is_east_cume_dist"),
        F.avg("prev_sqft_bucket").over(w8).alias("prev_sqft_bucket_avg_3"),
        F.sum("prev_sqft_bucket").over(w9).alias("running_prev_sqft_bucket"),
    )
)

result = (
    enriched
    .select(
        "id",
        "resolved_region",
        "last_known_is_east",
        "sqft_bucket",
        "prev_is_east",
        "prev_sqft_bucket",
        "next_sqft_bucket",
        "row_seq",
        "prev_sqft_bucket_avg_3",
    )
)
