from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("status_code").desc(), "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "log_ts",
        "status_code",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
    )
)

ranked = (
    flattened_attrs
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select("id", "log_ts", "status_code", "key", "status_code_high", "flag_bit8")
    .orderBy(F.col("status_code_high").desc_nulls_first())
)
