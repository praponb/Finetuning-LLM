from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("status_code", "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

ranked = (
    exploded
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

kept_keys = (
    ranked
    .select("service")
    .intersect(
        df_app_logs
        .filter(F.col("log_ts") >= "2025-08-04")
        .select("service")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("log_ts") > F.lit("2025-06-01").cast("date"))
    .withColumn("attr_key_level_key", F.concat_ws(",", "attr_key", "level"))
)

base_app_logs = (
    base
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & ~F.col("level").isin(["ERROR", "DEBUG", "WARN"]))
    .withColumn("months_since_log_ts", (F.datediff(F.current_date(), "log_ts") / 30))
)

result = (
    base_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "attr_key",
        "attr_value",
        "attr_key_level_key",
        F.when(F.col("level").rlike("^(WARN|INFO)$"), "CORE").when(F.col("level").rlike("^(ERROR|DEBUG)$"), "GROUP_A").otherwise("PRIORITY").alias("level_zone"),
    )
    .orderBy(F.col("log_ts").desc_nulls_last())
)
