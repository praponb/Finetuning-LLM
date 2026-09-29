from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-10-25").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.when(F.col("country").isin(["SG", "UK"]), "Priority").otherwise("Other").alias("country_group"),
        F.coalesce("country", F.lit("OTHER")).alias("resolved_country"),
    )
)

result = (
    cleaned
    .groupBy("browser")
    .agg(
        F.max("started_at").alias("last_seen"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.max("started_at").alias("ended_at"),
    )
    .orderBy("browser")
)
