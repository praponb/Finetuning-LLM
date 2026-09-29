from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "service", "status_code", F.explode("context").alias("attr_key", "v"))
)

result = (
    lines
    .select(
        "id",
        "status_code",
        "attr_key",
        "v",
        F.when(F.col("attr_key").rlike("^(version|trace)$"), "EXTENDED").when(F.col("attr_key").rlike("^(region)$"), "PRIORITY").otherwise("CORE").alias("attr_key_zone"),
    )
    .orderBy(F.col("id").asc_nulls_last(), F.col("attr_key_zone").desc())
)
