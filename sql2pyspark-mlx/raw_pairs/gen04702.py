from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "level").orderBy("status_code", "id", "attr_key")
w2 = Window.partitionBy("level").orderBy(F.col("status_code").desc(), "id", "attr_key")
w_grp = Window.partitionBy("host").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("level").orderBy("latest_rank", "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

latest = (
    exploded
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest_app_logs = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

latest_app_logs_2 = (
    latest_app_logs
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    latest_app_logs_2
    .filter(F.col("attr_key").isNotNull())
    .withColumn("attr_key_upper", F.upper("attr_key"))
    .withColumn("is_info", F.when(F.col("level") == "INFO", 1).otherwise(0))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("N/A")))
)

result = (
    cleaned
    .filter(F.col("service") == "search")
    .select(
        "id",
        "attr_key",
        "v",
        "rank_in_group",
        "attr_key_upper",
        F.first("attr_key", ignorenulls=True).over(w_grp).alias("first_known_attr_key"),
        F.row_number().over(w_part).alias("row_seq"),
        F.lag("is_info", 1, 0).over(w_part).alias("prev_is_info"),
    )
)
