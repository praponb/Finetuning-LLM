from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "West")
    .select("id", "opened_date")
    .subtract(df_stores.filter(F.col("sqft") > 8633).select("id", "opened_date"))
    .orderBy("id")
)
