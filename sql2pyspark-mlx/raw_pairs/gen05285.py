from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("has_fault").desc(), "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy(F.col("rn").desc(), "id", "attr_key")
w3 = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc())
w4 = Window.orderBy("status_code_high")
w5 = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key")
w6 = Window.partitionBy("attr_key").orderBy("event_seq", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = df_app_logs.select("id", "status_code", F.explode("context").alias("attr_key", "v"))

flattened_attrs = (
    exploded
    .select(
        "id",
        "status_code",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

latest = (
    flattened_attrs
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "status_code",
        "attr_key",
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w).alias("event_seq_2"),
        F.cume_dist().over(w3).alias("status_code_cume_dist"),
        F.rank().over(w4).alias("status_code_high_rank"),
        F.lead("status_code", 1).over(w5).alias("next_status_code"),
    )
)

result = (
    metrics
    .select(
        "id",
        "attr_key",
        "event_seq_2",
        "status_code_cume_dist",
        "status_code_high_rank",
        "next_status_code",
        F.sum("status_code_high_rank").over(w6).alias("running_sum_status_code_high_rank"),
        F.count("status_code_high_rank").over(w6).alias("running_count_status_code_high_rank"),
        F.max("status_code_high_rank").over(w6).alias("running_max_status_code_high_rank"),
    )
)
