from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("element"))
)

result = (
    lines
    .groupBy("currency")
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
    .filter(F.col("num_plans") < 1000)
    .orderBy(F.col("num_plans").asc_nulls_last())
)
