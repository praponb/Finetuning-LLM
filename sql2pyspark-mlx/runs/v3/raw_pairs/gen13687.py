from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter(((F.upper(F.trim("email")) == "CAROL@COMPANY.CO") & F.col("city").isNotNull()) | F.col("country").isin(["JP", "TH", "UK"]))
    .withColumn("is_active_label", F.when(F.col("is_active"), "Yes").otherwise("No"))
)

city_summary = (
    valid_customers
    .groupBy("city")
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.round(((100 * F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0))) / F.count("*")), 1).alias("pct_smb"),
    )
)

result = (
    city_summary
    .filter(F.col("stddev_age") >= 45)
    .select("city", "stddev_age", "pct_smb")
)
