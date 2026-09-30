from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("signup_date").desc_nulls_last(), "id")

cleaned = (
    df_customers
    .filter(F.col("city").isNotNull() & (F.col("signup_date") >= F.lit("2023-09-05").cast("date")))
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.last_day("signup_date").alias("signup_date_month_end"),
        F.coalesce("name", F.lit("N/A")).alias("name_filled"),
        F.coalesce("segment", F.lit("OTHER")).alias("segment_clean"),
    )
)

cleaned_customers = (
    cleaned
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "city",
        "segment",
        "signup_date",
        "age",
        "signup_date_month_end",
        "name_filled",
        "segment_clean",
        F.substring("country", 1, 3).alias("country_prefix"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

valid = (
    cleaned_customers
    .filter(F.col("signup_date_month_end").between("2023-05-01", "2023-09-23"))
    .select(
        "*",
        F.round((F.datediff("signup_date_month_end", "signup_date") / 30), 1).alias("months_signup_date_to_signup_date_month_end"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

valid_customers = (
    ranked
    .filter(F.col("city").isNotNull())
    .withColumn("clean_city", F.trim("city"))
    .withColumn("segment_country", F.concat("segment", F.lit("/"), "country"))
    .withColumn("city_clean", F.coalesce("city", F.lit("OTHER")))
)

result = (
    valid_customers
    .select(
        "id",
        "country",
        "signup_date",
        "age",
        "signup_date_month_end",
        "name_filled",
        "segment_clean",
        "country_prefix",
        "city_filled",
        F.when(F.col("country").rlike("^(JP|TH)$"), "CORE").when(F.col("country").rlike("^(US|SG)$"), "GROUP_A").otherwise("PRIORITY").alias("country_zone"),
    )
    .orderBy(F.col("country_zone").desc_nulls_last())
)
