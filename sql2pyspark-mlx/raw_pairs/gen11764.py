from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc_nulls_first(), "id")

filtered = (
    df_customers
    .filter(F.col("city").isNotNull() & (F.col("signup_date") > F.lit("2023-03-20").cast("date")))
    .select(
        "id",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        F.lower("city").alias("city_lower"),
        F.date_trunc("WEEK", "signup_date").alias("signup_date_period"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "city", "segment", "credit_limit", "city_lower", "signup_date_period")
)
