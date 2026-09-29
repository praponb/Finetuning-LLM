from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_type", "status").orderBy(F.col("opened_date").asc_nulls_last(), "id")
w3 = Window.partitionBy("account_type").orderBy("running_avg_is_checking", "id")
w_grp = Window.partitionBy("status").orderBy("row_num_2", "id")
w_running = Window.partitionBy("status", "account_type").orderBy("opened_date", "id")
w6 = Window.partitionBy(F.year("opened_date"), "status").orderBy("prev_running_avg_is_checking", "id")

valid = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]))
    .select(
        "*",
        F.when(F.col("account_type") == "CHECKING", 1).otherwise(0).alias("is_checking"),
    )
)

metrics = (
    valid
    .filter(F.col("opened_date") >= "2023-08-17")
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        F.max("is_checking").over(w).alias("running_max_is_checking"),
        F.avg("is_checking").over(w).alias("running_avg_is_checking"),
        F.min("is_checking").over(w).alias("running_min_is_checking"),
    )
)

deduped = (
    metrics
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

latest = (
    deduped
    .withColumn("row_num_2", F.row_number().over(w3))
    .filter(F.col("row_num_2") == 1)
)

with_windows = (
    latest
    .withColumn("prev_running_avg_is_checking", F.lag("running_avg_is_checking").over(w_grp))
    .withColumn("next_running_avg_is_checking", F.lead("running_avg_is_checking").over(w_grp))
    .withColumn("prev_running_min_is_checking", F.lag("running_min_is_checking").over(w_grp))
    .withColumn("next_running_min_is_checking", F.lead("running_min_is_checking").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_grp))
)

with_windows_accounts = (
    with_windows
    .withColumn("next_running_avg_is_checking_2", F.lead("running_avg_is_checking").over(w_running))
    .withColumn("next_next_running_min_is_checking", F.lead("next_running_min_is_checking").over(w_running))
    .withColumn("prev_next_running_min_is_checking", F.lag("next_running_min_is_checking").over(w_running))
    .withColumn("row_seq_2", F.row_number().over(w_running))
    .withColumn("row_seq_3", F.row_number().over(w6))
)

result = (
    with_windows_accounts
    .select(
        "id",
        "account_type",
        "next_running_avg_is_checking_2",
        F.col("prev_next_running_min_is_checking").alias("PREV_NEXT_RUNNING_MIN_IS_CHECKING_VAL"),
        F.col("running_max_is_checking").alias("RUNNING_MAX_IS_CHECKING"),
    )
    .unpivot(
        ids=["id", "account_type"],
        values=["next_running_avg_is_checking_2", "PREV_NEXT_RUNNING_MIN_IS_CHECKING_VAL", "RUNNING_MAX_IS_CHECKING"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
