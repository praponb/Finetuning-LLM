from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status_code_tier").orderBy(F.col("num_services").desc())

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context")["trace"].alias("trace"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("status_code") >= 451, "LARGE").otherwise("SMALL").alias("status_code_tier"),
        "host",
    )
    .agg(
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_rows"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.sort_array(F.collect_set("tier")).alias("tier_list"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_rows"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("service") == "search", 1)), F.lit(0)), 2).alias("status_code_per_search"),
        F.size(F.collect_set("service")).alias("num_services"),
    )
    .filter(F.col("num_services") < 1)
    .withColumn("num_services_dense_rank", F.dense_rank().over(w))
)
