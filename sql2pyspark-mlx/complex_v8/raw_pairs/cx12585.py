from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_id_by_status_l = Window.partitionBy("status", "holder_id").orderBy(F.col("status_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_holder_id_by_status_l_2 = Window.partitionBy("status", "holder_id").orderBy(F.col("status_len").desc(), "id")
w_holder_id = Window.partitionBy("holder_id")
w_status_holder_id_by_status_l_3 = Window.partitionBy("status", "holder_id").orderBy(F.col("status_len").desc(), "id").rowsBetween(-2, Window.currentRow)
w_status_len_by_end_date = Window.partitionBy("status_len").orderBy(F.col("end_date").desc(), "id")

recent_policys = (
    df_policies
    .filter((F.col("product") == "AUTO") & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("start_date")).alias("days_since_start_date"),
        F.concat_ws(" / ", F.col("status"), F.col("product")).alias("status_product_label"),
    )
)

metrics = (
    recent_policys
    .select(
        "*",
        F.nth_value("status_len", 2).over(w_status_holder_id_by_status_l).alias("second_status_len"),
        F.lag("premium", 1, 0).over(w_status_holder_id_by_status_l_2).alias("prev_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id), 2).alias("premium_vs_holder_id_avg"),
        F.round(F.avg("status_len").over(w_status_holder_id_by_status_l_3), 2).alias("status_len_avg_3"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_status_len_by_end_date)).alias("days_since_prev_end_date"),
    )
)

result = (
    metrics
    .groupBy("holder_id")
    .pivot("status", ["ACTIVE", "LAPSED", "EXPIRED"])
    .agg(F.sum("premium"))
)
