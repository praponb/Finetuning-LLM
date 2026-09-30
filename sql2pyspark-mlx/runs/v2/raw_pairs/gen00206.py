from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "DE") & (F.col("city") == "Chiang Mai"))
    .select(
        "city",
        F.regexp_replace("email", " ", "").alias("email_slug"),
        F.trim("segment").alias("clean_segment"),
    )
)
