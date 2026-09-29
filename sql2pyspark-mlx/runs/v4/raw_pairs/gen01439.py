from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

valid = (
    lines
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .withColumn("clean_tag", F.trim("tag"))
    .withColumn("created_at_day", F.dayofmonth("created_at"))
)

cleaned = (
    valid
    .filter(F.col("customer_id").isNotNull() & F.hour("created_at").between(6, 10))
    .select("*", F.initcap(F.trim("tag")).alias("tag_title"))
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.email"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("created_at_day") >= 69, "PREMIUM").otherwise("STANDARD").alias("created_at_day_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min_2"),
        F.count(F.when(F.col("tag") == "WELCOME5", 1)).alias("welcome5_count"),
        F.round(F.avg("created_at_day"), 2).alias("avg_created_at_day"),
    )
    .orderBy(F.col("duration_min_2").desc_nulls_first())
)
