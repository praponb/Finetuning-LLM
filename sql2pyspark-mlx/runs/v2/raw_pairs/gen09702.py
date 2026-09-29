from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "London") & (F.col("sqft") != 11779) & (F.col("city") == "Singapore"))
    .groupBy("city")
    .agg(F.min("opened_date").alias("first_opened_date"))
)
