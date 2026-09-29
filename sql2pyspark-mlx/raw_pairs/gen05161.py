from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("key", "host").orderBy("status_code_high", "id", "key")
w2 = Window.partitionBy("host").orderBy(F.col("status_code_high").desc_nulls_first())
w3 = Window.partitionBy("host").orderBy(F.col("next_status_code_high").desc_nulls_first(), "id", "key")
w4 = Window.partitionBy("key").orderBy("status_code_high", "id", "key")
w5 = Window.partitionBy("key").orderBy("prev_flag_bit8", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("key").orderBy(F.col("next_prev_flag_bit8").desc_nulls_first(), "id", "key")
w7 = Window.partitionBy("key").orderBy(F.col("status_code_high").desc(), "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "host",
        "key",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    flattened_attrs
    .select(
        "id",
        "host",
        "key",
        "status_code_high",
        F.lead("status_code_high").over(w_recent).alias("next_status_code_high"),
        F.lag("status_code_high").over(w_recent).alias("prev_status_code_high"),
        F.lag("flag_bit8").over(w_recent).alias("prev_flag_bit8"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.cume_dist().over(w2).alias("status_code_high_cume_dist"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "key", "status_code_high", "prev_flag_bit8")
)

enriched = (
    latest
    .withColumn("next_status_code_high", F.lead("status_code_high").over(w4))
    .withColumn("next_prev_flag_bit8", F.lead("prev_flag_bit8").over(w4))
    .withColumn("prev_prev_flag_bit8", F.lag("prev_flag_bit8").over(w4))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("first_known_key", F.first("key", ignorenulls=True).over(w5))
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest_app_logs = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest_app_logs
    .select(
        "id",
        "key",
        "prev_flag_bit8",
        "next_status_code_high",
        "prev_prev_flag_bit8",
        "event_seq",
    )
    .orderBy(F.col("next_status_code_high").desc(), "event_seq")
)
