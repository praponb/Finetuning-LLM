from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("code"))

filtered = (
    exploded
    .filter(F.col("created_at") > F.lit("2025-03-02 00:00:00").cast("timestamp"))
    .withColumn("code_prefix", F.substring("code", 1, 2))
)

cleaned = (
    filtered
    .filter(F.col("created_at") > F.lit("2025-03-02").cast("date"))
    .select("*", F.upper("code").alias("code_upper"))
)

valid = (
    cleaned
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-01"))) & (F.col("code") != "WELCOME5"))
    .withColumn("code_title", F.initcap(F.trim("code")))
    .withColumn("created_at_hour", F.hour("created_at"))
)

result = (
    valid
    .select(
        "code",
        "code_prefix",
        "code_upper",
        "code_title",
        "created_at_hour",
        F.when(F.col("code").rlike("^(SPRING10|WELCOME5)$"), "EXTENDED").otherwise("CORE").alias("code_zone"),
    )
    .orderBy("code_zone")
)
