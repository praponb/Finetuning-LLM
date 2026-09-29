from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") < "2025-03-01") & (F.length("email") >= 12))
    .select("segment", "name", F.coalesce("city", F.lit("N/A")).alias("city_filled"))
    .orderBy("city_filled", "segment")
)
