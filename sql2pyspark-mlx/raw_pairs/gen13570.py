from pyspark.sql import functions as F

valid = (
    df_customers
    .filter(~F.col("city").eqNullSafe(F.lit("Chiang Mai")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.when(F.col("city").isin(["Bangkok", "London"]), "Tier 1").otherwise("Other").alias("city_group"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("credit_limit") >= 13000, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
    )
    .agg(
        F.count_if(F.col("segment") == "SMB").alias("smb_events"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("credit_limit")).otherwise(0)).alias("berlin_credit_limit"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_rows"),
    )
    .orderBy(F.col("berlin_credit_limit").desc())
)
