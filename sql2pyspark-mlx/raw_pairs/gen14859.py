from pyspark.sql import functions as F

flattened = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("code"))

valid = (
    flattened
    .filter(F.col("customer_id") == 32)
    .withColumn("code_prefix", F.substring("code", 1, 4))
    .withColumn("code_lower", F.lower("code"))
    .withColumn("resolved_code", F.coalesce("code", F.lit("DIRECT")))
)

filtered = (
    valid
    .filter(F.col("customer_id") == 9)
    .select(
        "*",
        F.when(F.col("code").isin(["BUNDLE", "SPRING10"]), "Group A").otherwise("Other").alias("code_group"),
    )
)

enriched = (
    filtered
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.segment"), F.col("d.signup_date"))
)

result = (
    enriched
    .groupBy("segment")
    .agg(
        F.count("*").alias("num_rows"),
        F.count_if(F.col("code") == "BUNDLE").alias("bundle_events"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"),
    )
)
