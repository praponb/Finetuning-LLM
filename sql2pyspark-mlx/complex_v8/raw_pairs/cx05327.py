from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh_ties = Window.orderBy("kwh")
w_all_by_kwh = Window.orderBy("kwh", "id")
w_site_id_by_kwh_cume_dist_mov = Window.partitionBy("site_id").orderBy(F.col("kwh_cume_dist").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_site_id_by_kwh_cume_dist = Window.partitionBy("site_id").orderBy(F.col("kwh_cume_dist").desc_nulls_first(), "id")

scoped = (
    df_meter_readings
    .filter(~F.col("quality").isin("OK", "EST"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
    )
)

sequenced = (
    scoped
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_kwh_ties), 3).alias("kwh_cume_dist"),
        F.lead("read_at").over(w_all_by_kwh).alias("next_read_at"),
    )
)

ranked = (
    sequenced
    .select(
        "*",
        F.max("kwh_bucket").over(w_site_id_by_kwh_cume_dist_mov).alias("kwh_bucket_max_3"),
        F.lag("kwh_cume_dist", 1, 0).over(w_site_id_by_kwh_cume_dist).alias("prev_kwh_cume_dist"),
    )
)

result = (
    ranked
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "quality_ref",
        "kwh_bucket",
        "read_at_label",
        "kwh_cume_dist",
        "kwh_bucket_max_3",
        "prev_kwh_cume_dist",
    )
    .orderBy(F.col("kwh_bucket_max_3").desc_nulls_first(), F.col("kwh").desc())
)
