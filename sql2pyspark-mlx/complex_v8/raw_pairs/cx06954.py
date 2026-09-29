from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_year_status_by_star = Window.partitionBy(F.year("end_date"), "status").orderBy("start_date", "id")
w_end_date_year_status_by_star_2 = Window.partitionBy(F.year("end_date"), "status").orderBy("start_date")

filtered = (
    df_policies
    .filter((F.col("premium") > 1042.8) | (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.ntile(3).over(w_end_date_year_status_by_star).alias("start_date_tercile"),
        F.dense_rank().over(w_end_date_year_status_by_star_2).alias("start_date_dense_rank"),
        F.lag("end_date").over(w_end_date_year_status_by_star).alias("prev_end_date"),
    )
)

result = (
    metrics
    .rollup("status", "holder_id")
    .agg(
        F.grouping("status").alias("is_total_status"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.round(100 * F.count(F.when(F.col("status") == "ACTIVE", 1)) / F.count("*"), 1).alias("pct_status_active"),
    )
)
