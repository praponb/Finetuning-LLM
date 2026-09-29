from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("page").orderBy("duration_sec_scaled", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("page").orderBy(F.col("duration_sec_scaled").asc_nulls_last())
w_ordered = Window.orderBy("duration_sec_scaled_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "page",
        F.initcap(F.trim("page")).alias("page_title"),
        F.round((F.col("duration_sec") * 100), 2).alias("duration_sec_scaled"),
    )
)

metrics = (
    cleaned
    .filter(F.col("page") != "/cart")
    .select(
        "id",
        "page_title",
        F.count("duration_sec_scaled").over(w_hist).alias("last7_count_duration_sec_scaled"),
        F.min("duration_sec_scaled").over(w_hist).alias("last7_min_duration_sec_scaled"),
        F.avg("duration_sec_scaled").over(w_hist).alias("last7_avg_duration_sec_scaled"),
        F.cume_dist().over(w2).alias("duration_sec_scaled_cume_dist"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "page_title",
        F.max("duration_sec_scaled_cume_dist").over(w_ordered).alias("running_max_duration_sec_scaled_cume_dist"),
        F.sum("duration_sec_scaled_cume_dist").over(w_ordered).alias("running_sum_duration_sec_scaled_cume_dist"),
        F.min("duration_sec_scaled_cume_dist").over(w_ordered).alias("running_min_duration_sec_scaled_cume_dist"),
        F.avg("duration_sec_scaled_cume_dist").over(w_ordered).alias("running_avg_duration_sec_scaled_cume_dist"),
    )
)

base = (
    enriched
    .filter(F.col("running_sum_duration_sec_scaled_cume_dist") >= 2.5)
    .select(
        "*",
        (F.col("running_min_duration_sec_scaled_cume_dist") - F.col("running_max_duration_sec_scaled_cume_dist")).alias("running_min_duration_sec_scaled_cume_dist_minus_running_max_duration_sec_scaled_cume_dist"),
        F.round((F.col("running_max_duration_sec_scaled_cume_dist") * 0.9), 2).alias("running_max_duration_sec_scaled_cume_dist_adjusted"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("running_max_duration_sec_scaled_cume_dist_adjusted") >= 78, "LARGE").otherwise("SMALL").alias("running_max_duration_sec_scaled_cume_dist_adjusted_tier"),
    )
    .agg(
        F.sum("running_max_duration_sec_scaled_cume_dist_adjusted").alias("total_running_max_duration_sec_scaled_cume_dist_adjusted"),
        F.count("*").alias("cnt"),
    )
)
