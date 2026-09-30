from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("segment")
w3 = Window.partitionBy("segment", "country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city", "segment").orderBy("credit_limit", "id")
w5 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("segment", "city").orderBy(F.col("prev_age").desc(), "id")
w_grp = Window.partitionBy("city").orderBy("last_known_age_times_credit_limit_avg_5_avg_5", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("segment", "city").orderBy("last_known_age_times_credit_limit_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.partitionBy("segment").orderBy(F.col("last_known_age").desc())
w10 = Window.partitionBy("segment").orderBy("last_known_age_times_credit_limit_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("city").orderBy(F.col("last_known_age_times_credit_limit_avg_5_avg_5").desc(), "id")

with_windows = (
    df_customers
    .select(
        "*",
        F.avg("credit_limit").over(w).alias("credit_limit_avg_5"),
        F.corr("credit_limit", "age").over(w2).alias("corr_credit_limit_age"),
        F.last("age", ignorenulls=True).over(w3).alias("last_known_age"),
        F.lag("age", 1, 0).over(w4).alias("prev_age"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("signup_date") > F.lit("2023-07-12").cast("date"))
    .select(
        "*",
        (F.col("last_known_age") * F.col("credit_limit_avg_5")).alias("last_known_age_times_credit_limit_avg_5"),
        F.concat("country", F.lit(", "), "email").alias("country_email"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
    )
)

enriched = (
    cleaned
    .withColumn("last_known_age_times_credit_limit_avg_5_avg_5", F.avg("last_known_age_times_credit_limit_avg_5").over(w5))
    .withColumn("row_seq", F.row_number().over(w6))
)

metrics = (
    enriched
    .select(
        "*",
        F.avg("last_known_age_times_credit_limit_avg_5").over(w_grp).alias("last_known_age_times_credit_limit_avg_5_avg_3"),
        F.first("corr_credit_limit_age", ignorenulls=True).over(w8).alias("first_known_corr_credit_limit_age"),
        F.percent_rank().over(w_grp_2).alias("last_known_age_percent_rank"),
        F.first("row_seq", ignorenulls=True).over(w10).alias("first_known_row_seq"),
        F.row_number().over(w11).alias("row_seq_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "age",
        "credit_limit_avg_5",
        F.when(F.col("segment").rlike("^(SMB|Consumer)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("segment_zone"),
    )
    .orderBy(F.col("signup_date").desc_nulls_first())
)
