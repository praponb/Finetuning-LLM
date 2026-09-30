from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "JP")
    .select("city")
    .unionByName(df_stores.filter(F.col("sqft") > 4723).select("city"))
    .distinct()
    .orderBy("city")
)
