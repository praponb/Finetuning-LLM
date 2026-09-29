from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("age", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("WEEK", "signup_date"), "city")

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-05-23").cast("date")) & F.col("city").isNotNull())
    .select(
        "*",
        F.trim("city").alias("clean_city"),
        F.initcap(F.trim("city")).alias("city_title"),
    )
)

result = (
    cleaned
    .filter(F.col("country") == "US")
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "is_active",
        "age",
        "city_title",
        F.min("credit_limit").over(w).alias("last4_min_credit_limit"),
        F.sum("credit_limit").over(w).alias("last4_sum_credit_limit"),
        F.avg("credit_limit").over(w).alias("last4_avg_credit_limit"),
        F.max("age").over(w2).alias("signup_date_period_max_age"),
    )
)
