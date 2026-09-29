from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("month", "signup_date_plus_months"), "country").orderBy(F.col("segment_length").asc_nulls_last())
w3 = Window.partitionBy("country").orderBy("signup_date_plus_months", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("segment").orderBy("age", "id")

filtered = (
    df_customers
    .filter(F.col("city").isNotNull() & (F.col("signup_date") >= "2025-08-25"))
    .select(
        "id",
        "name",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.length("segment").alias("segment_length"),
        F.coalesce("segment", F.lit("NONE")).alias("segment_filled"),
    )
)

valid = (
    filtered
    .filter((F.col("signup_date") > F.lit("2023-08-04").cast("date")) & (F.col("age") >= 56))
    .withColumn("clean_country", F.trim("country"))
    .withColumn("signup_date_plus_months", F.add_months("signup_date", 6))
    .withColumn("resolved_segment", F.coalesce("segment", F.lit("DIRECT")))
)

metrics = (
    valid
    .filter(F.col("segment").isin(["Corporate", "Consumer", "SMB"]))
    .select(
        "*",
        F.first("segment_length", ignorenulls=True).over(w).alias("first_known_segment_length"),
        F.rank().over(w2).alias("segment_length_rank"),
        F.avg("age").over(w3).alias("age_avg_3"),
        F.lead("segment_length", 1).over(w4).alias("next_segment_length"),
    )
)

cleaned = (
    metrics
    .filter(F.col("country").isin(["US", "JP"]))
    .select(
        "*",
        F.add_months("signup_date_plus_months", 1).alias("signup_date_plus_months_plus_months"),
    )
)

cleaned_customers = (
    cleaned
    .filter((F.col("signup_date") > F.lit("2023-04-25").cast("date")) & (F.col("age") > 63))
    .select(
        "*",
        F.round((F.col("first_known_segment_length") / F.col("age")), 2).alias("first_known_segment_length_per_age"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled_2"),
    )
)

result = (
    cleaned_customers
    .withColumn("quarter_start", F.date_trunc("QUARTER", "signup_date"))
    .withColumn("segment_length_tier", F.when(F.col("segment_length") >= 74, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["quarter_start", "segment_length_tier"], ["quarter_start"], []],
        "quarter_start",
        "segment_length_tier",
    )
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("first_known_segment_length_per_age").alias("avg_first_known_segment_length_per_age"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.max(F.when(F.col("segment") == "SMB", F.col("age_avg_3"))).alias("smb_max_age_avg_3"),
        F.grouping("quarter_start").alias("grouping_level"),
    )
    .orderBy(F.col("quarter_start").asc_nulls_first())
)
