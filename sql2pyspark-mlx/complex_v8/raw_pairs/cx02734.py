from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region_segment_by_joined_on = Window.partitionBy("region", "segment").orderBy("joined_on", "id")
w_all_by_joined_on = Window.orderBy("joined_on", "id")
w_all_by_joined_on_full = Window.orderBy("joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_holders = (
    df_policyholders
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.date_format(F.col("joined_on"), "yyyy-MM").alias("joined_on_label"),
    )
)

enriched = (
    filtered_holders
    .filter(F.col("joined_on") >= F.lit("2024-06-29").cast("date"))
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "segment",
        "joined_on_label",
        F.lower(F.regexp_replace(F.trim(F.col("holder_name")), " ", "_")).alias("holder_name_slug"),
    )
)

ranked = (
    enriched
    .select(
        "id",
        "holder_name",
        "joined_on",
        "segment",
        "joined_on_label",
        "holder_name_slug",
        F.round(F.percent_rank().over(w_region_segment_by_joined_on), 3).alias("joined_on_percent_rank"),
        F.lag("joined_on", 2).over(w_region_segment_by_joined_on).alias("prev2_joined_on"),
        F.lag("joined_on", 3).over(w_all_by_joined_on).alias("prev3_joined_on"),
        F.lag("joined_on").over(w_region_segment_by_joined_on).alias("prev_joined_on"),
        F.last("joined_on").over(w_all_by_joined_on_full).alias("last_joined_on"),
    )
)

result = (
    ranked
    .select(
        "id",
        "holder_name",
        "joined_on",
        "segment",
        "joined_on_label",
        "holder_name_slug",
        "joined_on_percent_rank",
        "prev2_joined_on",
        "prev3_joined_on",
        "last_joined_on",
    )
    .orderBy(F.col("prev3_joined_on").asc(), "id")
    .limit(10)
)
