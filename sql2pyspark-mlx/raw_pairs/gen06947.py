from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("attr_key").orderBy("host_length", "id", "attr_key").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("attr_key").orderBy("last3_avg_host_length", "id", "attr_key")

lines = (
    df_app_logs
    .select("id", "service", "host", F.explode("context").alias("attr_key", "value"))
)

cleaned = (
    lines
    .filter(~F.col("host").isin(["h4", "h1"]))
    .withColumn("host_lower", F.lower("host"))
)

valid = (
    cleaned
    .filter(F.col("host") == "h3")
    .select(
        "id",
        "service",
        "attr_key",
        "value",
        "host_lower",
        F.length("host").alias("host_length"),
        F.when(F.col("service").isin(["checkout", "billing"]), "Group A").otherwise("Other").alias("service_group"),
        F.coalesce("attr_key", F.lit("N/A")).alias("resolved_attr_key"),
    )
)

enriched = (
    valid
    .filter(F.col("service") != "search")
    .select(
        "id",
        "attr_key",
        "value",
        "host_lower",
        "host_length",
        "resolved_attr_key",
        F.count("host_length").over(w_ordered).alias("last3_count_host_length"),
        F.avg("host_length").over(w_ordered).alias("last3_avg_host_length"),
        F.max("host_length").over(w_ordered).alias("last3_max_host_length"),
    )
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "attr_key", "host_lower", "resolved_attr_key")
)

result = (
    ranked
    .select(
        "id",
        "attr_key",
        "host_lower",
        "resolved_attr_key",
        F.when(F.col("attr_key").rlike("^(version|region)$"), "GROUP_B").when(F.col("attr_key").rlike("^(trace)$"), "PRIORITY").otherwise("GROUP_A").alias("attr_key_zone"),
    )
    .orderBy(F.col("attr_key_zone").asc_nulls_first())
)
