from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_by_commissioned_on = Window.partitionBy("zone").orderBy("commissioned_on", "id")
w_zone_by_commissioned_on_2 = Window.partitionBy("zone").orderBy(F.col("commissioned_on").desc_nulls_first(), "id")
w_zone_by_commissioned_on_full = Window.partitionBy("zone").orderBy(F.col("commissioned_on").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_sites
    .filter(F.col("commissioned_on").between(F.lit("2024-08-24").cast("date"), F.lit("2025-05-10").cast("date")))
    .select(
        "id",
        "site_name",
        "zone",
        "commissioned_on",
        F.substring(F.trim(F.col("zone")), 1, 4).alias("zone_prefix"),
    )
)

sequenced = (
    prepared
    .select(
        "id",
        "zone",
        "commissioned_on",
        F.rank().over(w_zone_by_commissioned_on).alias("commissioned_on_rank"),
        F.ntile(10).over(w_zone_by_commissioned_on_2).alias("commissioned_on_decile"),
        F.row_number().over(w_zone_by_commissioned_on).alias("rn"),
        F.dense_rank().over(w_zone_by_commissioned_on_2).alias("commissioned_on_dense_rank"),
        F.datediff(F.col("commissioned_on"), F.lag("commissioned_on").over(w_zone_by_commissioned_on)).alias("days_since_prev_commissioned_on"),
        F.last("commissioned_on").over(w_zone_by_commissioned_on_full).alias("last_commissioned_on"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["rn", "commissioned_on_dense_rank"], ["rn"], ["commissioned_on_dense_rank"]],
        "rn", "commissioned_on_dense_rank",
    )
    .agg(
        F.grouping("rn").alias("is_total_rn"),
        F.sum(F.when(F.col("zone").isNotNull(), F.col("rn")).otherwise(0)).alias("rn_zone_is_not_null"),
        F.max("commissioned_on_decile").alias("max_commissioned_on_decile"),
        F.round(F.avg("commissioned_on_rank"), 2).alias("avg_commissioned_on_rank"),
    )
    .orderBy(
        F.col("rn").asc_nulls_last(),
        F.col("commissioned_on_dense_rank").asc_nulls_last(),
    )
)
