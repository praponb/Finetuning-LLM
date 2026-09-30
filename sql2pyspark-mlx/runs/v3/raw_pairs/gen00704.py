from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") <= "2023-07-01")
    .select("id", "sqft")
    .intersect(df_stores.filter(F.col("sqft") != 12990).select("id", "sqft"))
)
