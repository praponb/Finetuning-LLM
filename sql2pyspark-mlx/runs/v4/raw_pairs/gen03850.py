from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").asc_nulls_last())
w2 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(-3, -1)
w_part = Window.partitionBy("country")
w4 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_customers
    .filter(F.col("name").like("Emm%"))
    .select(
        "id",
        "name",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
    )
)

filtered = (
    valid
    .filter((F.col("signup_date") > F.lit("2023-04-21").cast("date")) & F.col("segment").isNotNull())
    .withColumn("segment_length", F.length("segment"))
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    ranked
    .select(
        "*",
        F.avg("is_active_flag").over(w2).alias("is_active_flag_avg_3_prior"),
        F.avg("credit_limit_filled").over(w_part).alias("country_avg_credit_limit_filled"),
        F.first("age", ignorenulls=True).over(w4).alias("first_known_age"),
    )
)

totals = (
    with_windows
    .groupBy(
        F.when(F.col("first_known_age") >= 72, "HIGH").otherwise("LOW").alias("first_known_age_tier"),
        "country",
        "segment",
    )
    .agg(
        F.sum(F.when(F.col("segment") == "Corporate", F.col("country_avg_credit_limit_filled")).otherwise(0)).alias("corporate_country_avg_credit_limit_filled"),
        F.percentile_approx("is_active_flag_avg_3_prior", 0.5).alias("is_active_flag_avg_3_prior_p50"),
        F.round(F.avg("first_known_age"), 2).alias("avg_first_known_age"),
        F.max(F.when(F.col("segment") == "SMB", F.col("first_known_age"))).alias("smb_max_first_known_age"),
        F.corr("age", "credit_limit").alias("corr_age_credit_limit"),
        F.round(F.sum("credit_limit_filled") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("credit_limit_filled_per_de"),
    )
)

result = (
    totals
    .select(
        "first_known_age_tier",
        "country",
        "segment",
        "is_active_flag_avg_3_prior_p50",
        "smb_max_first_known_age",
        "corr_age_credit_limit",
        "credit_limit_filled_per_de",
        F.when(F.col("country").rlike("^(SG|JP)$"), "PRIORITY").when(F.col("country").rlike("^(TH|US)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("country_zone"),
    )
    .orderBy(F.col("segment").desc_nulls_last())
)
