from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("flag_bit8_plus_status_code_high").desc_nulls_first())
w2 = Window.orderBy(F.col("flag_bit8_plus_status_code_high_cume_dist").asc_nulls_last())

exploded = (
    df_app_logs
    .select("id", "service", "status_code", F.explode_outer("context").alias("k", "v"))
)

parsed = (
    exploded
    .select(
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
    )
)

base = (
    parsed
    .filter(F.col("status_code_high") == 1)
    .withColumn("flag_bit8_plus_status_code_high", (F.col("flag_bit8") + F.col("status_code_high")))
)

scored = base.withColumn("flag_bit8_plus_status_code_high_cume_dist", F.cume_dist().over(w))

with_windows = (
    scored
    .filter(F.col("flag_bit8") > 1)
    .withColumn("flag_bit8_plus_status_code_high_cume_dist_dense_rank", F.dense_rank().over(w2))
)

base_app_logs = (
    with_windows
    .filter(F.col("flag_bit8_plus_status_code_high") < 36)
    .select(
        "*",
        (F.col("status_code_high") + F.col("flag_bit8_plus_status_code_high_cume_dist_dense_rank")).alias("status_code_high_plus_flag_bit8_plus_status_code_high_cume_dist_dense_rank"),
        F.col("status_code_high").cast("string").alias("status_code_high_str"),
    )
)

result = (
    base_app_logs
    .select(
        "v",
        "status_code_high",
        "flag_bit8",
        "flag_bit8_plus_status_code_high_cume_dist",
        "flag_bit8_plus_status_code_high_cume_dist_dense_rank",
    )
    .orderBy("flag_bit8_plus_status_code_high_cume_dist_dense_rank")
)
