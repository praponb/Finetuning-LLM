from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["London", "Tokyo", "Berlin"]))
    .select("id", "opened_date", "region", "sqft")
    .orderBy("sqft", "region")
)
