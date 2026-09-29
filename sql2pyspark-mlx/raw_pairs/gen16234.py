from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-08-21 02:00:00").cast("timestamp")) & (F.col("country") == "UK"))
    .withColumn("country_group", F.when(F.col("country").isin(["DE", "UK"]), "Core").otherwise("Other"))
    .withColumn("country_group_2", F.when(F.col("country").isin(["SG", "US"]), "Group A").otherwise("Other"))
    .withColumn("browser_clean", F.coalesce("browser", F.lit("UNKNOWN")))
)

result = (
    filtered
    .groupingSets([["device", "browser"], ["device"], []], "device", "browser")
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.min("started_at").alias("first_started_at"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
    )
    .filter(F.col("span_minutes") > 0)
    .orderBy(F.col("device").asc_nulls_last())
)
