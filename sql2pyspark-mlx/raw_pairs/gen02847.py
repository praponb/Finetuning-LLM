from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "attr_key").orderBy(F.col("status_code").desc(), "id", "attr_key")
w_ordered = Window.partitionBy("level").orderBy("status_code", "id", "attr_key").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("attr_key").orderBy(F.col("dedup_rank").desc(), "id", "attr_key")
w_grp = Window.orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("attr_key")
w6 = Window.orderBy(F.col("last3_min_status_code").desc_nulls_last())
w_part = Window.partitionBy("attr_key").orderBy("last3_avg_status_code", "id", "attr_key").rowsBetween(-3, -1)
w_recent = Window.partitionBy("attr_key").orderBy("row_num", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

latest = (
    lines
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    latest
    .select(
        "id",
        "status_code",
        "attr_key",
        "value",
        "dedup_rank",
        F.sum("status_code").over(w_ordered).alias("last3_sum_status_code"),
        F.min("status_code").over(w_ordered).alias("last3_min_status_code"),
        F.max("status_code").over(w_ordered).alias("last3_max_status_code"),
        F.avg("status_code").over(w_ordered).alias("last3_avg_status_code"),
    )
)

latest_app_logs = (
    with_windows
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

scored = (
    latest_app_logs
    .filter(F.col("last3_avg_status_code") > 400)
    .select(
        "*",
        F.last("last3_min_status_code", ignorenulls=True).over(w_grp).alias("last_known_last3_min_status_code"),
        F.round((F.col("last3_avg_status_code") / F.sum("last3_avg_status_code").over(w5)), 4).alias("last3_avg_status_code_share"),
        F.rank().over(w6).alias("last3_min_status_code_rank"),
        F.avg("last3_avg_status_code").over(w_part).alias("last3_avg_status_code_avg_3_prior"),
        F.first("last3_avg_status_code", ignorenulls=True).over(w_recent).alias("first_known_last3_avg_status_code"),
    )
)

result = (
    scored
    .select(
        "id",
        "status_code",
        "attr_key",
        "dedup_rank",
        "last3_min_status_code",
        "last3_max_status_code",
        "row_num",
        "last3_avg_status_code_share",
        "last3_min_status_code_rank",
    )
    .orderBy("status_code")
)
