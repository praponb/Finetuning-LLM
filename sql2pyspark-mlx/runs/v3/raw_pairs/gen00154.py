from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Berlin", "London", "Tokyo"]))
    .select("sqft", F.add_months("opened_date", 12).alias("opened_date_plus_months"))
    .orderBy(F.col("sqft").desc())
)
