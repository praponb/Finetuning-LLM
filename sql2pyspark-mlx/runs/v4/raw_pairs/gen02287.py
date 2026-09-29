from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("service").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_app_logs
    .filter(F.col("service") == "search")
    .select(
        "id",
        "service",
        "log_ts",
        "payload_json",
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    unpacked
    .withColumn("first_known_user_tier", F.first("user_tier", ignorenulls=True).over(w))
    .withColumn("first_known_region_or_default", F.first("region_or_default", ignorenulls=True).over(w2))
)
