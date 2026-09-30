from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("is_flagged", "id")
w_ordered = Window.partitionBy("level").orderBy("is_flagged", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .filter(F.col("level") == "INFO")
    .select(
        "id",
        "level",
        "payload_json",
        F.coalesce(F.col("context").getItem("region"), F.lit("none")).alias("region_or_default"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.col("context")["region"].alias("region"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("level") == "WARN")
    .select(
        "id",
        "region",
        F.lag("is_flagged", 1, 0).over(w).alias("prev_is_flagged"),
        F.last("region", ignorenulls=True).over(w_ordered).alias("last_known_region"),
    )
)
