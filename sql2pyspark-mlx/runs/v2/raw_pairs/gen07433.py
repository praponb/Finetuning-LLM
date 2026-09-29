from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(10006, 11061))
    .select("city")
    .unionByName(df_stores.filter(F.col("city") == "New York").select("city"))
    .orderBy("city")
)
