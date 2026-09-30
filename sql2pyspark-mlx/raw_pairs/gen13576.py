from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.partitionBy(F.date_trunc("WEEK", "signup_date"), "segment")
w3 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("age").desc_nulls_last())

base = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("email")).alias("email_title"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

result = (
    base
    .filter(F.col("credit_limit") <= 7900)
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "email_title",
        F.avg("credit_limit").over(w).alias("segment_avg_credit_limit"),
        F.avg("age").over(w2).alias("signup_date_period_avg_age"),
        F.sum("age").over(w3).alias("cumulative_age"),
        F.cume_dist().over(w4).alias("age_cume_dist"),
    )
)
