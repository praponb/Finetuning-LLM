from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "signup_date"), "country").orderBy("days_since_signup_date", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy(F.year("signup_date"), "city").orderBy("age", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("country")

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-08-03").cast("date")) & F.col("city").isin(["New York", "Tokyo", "Singapore"]))
    .withColumn("days_since_signup_date", F.datediff(F.current_date(), "signup_date"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("NONE")))
)

result = (
    cleaned
    .filter(F.col("signup_date") <= "2025-05-01")
    .select(
        "*",
        F.avg("days_since_signup_date").over(w).alias("days_since_signup_date_avg_3_prior"),
        F.avg("age").over(w2).alias("age_avg_6"),
        F.stddev_samp("credit_limit").over(w3).alias("country_stddev_samp_credit_limit"),
    )
)
