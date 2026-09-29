from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc_nulls_last(), "id")

cleaned = (
    df_customers
    .filter(F.month("signup_date").isin([1, 4, 12]))
    .select(
        "*",
        F.regexp_replace("email", " ", "_").alias("email_slug"),
        F.concat_ws("-", "email", "country").alias("email_country_key"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

result = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
