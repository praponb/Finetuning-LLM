from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "key",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    flattened_attrs
    .select(
        "id",
        "service",
        "host",
        "status_code_high",
        F.avg("status_code_high").over(w).alias("service_avg_status_code_high"),
    )
)

base = (
    metrics
    .filter(~F.col("service").isin(["search", "auth", "checkout"]))
    .withColumn("host_service", F.concat("host", F.lit(", "), "service"))
)

filtered = (
    base
    .filter(~F.col("service").isin(["billing", "search", "auth"]))
    .withColumn("service_avg_status_code_high_adjusted", F.round((F.col("service_avg_status_code_high") * 1.1), 2))
)

result = (
    filtered
    .select(
        "id",
        "host",
        "status_code_high",
        "service_avg_status_code_high",
        "host_service",
        "service_avg_status_code_high_adjusted",
        F.when(F.col("host").rlike("^(h2|h4)$"), "PRIORITY").when(F.col("host").rlike("^(h1|h3)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("host_zone"),
    )
    .orderBy(F.col("service_avg_status_code_high").asc_nulls_last())
)
