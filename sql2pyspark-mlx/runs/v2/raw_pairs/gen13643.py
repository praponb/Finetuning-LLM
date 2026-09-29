from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city")
    .intersect(df_stores.filter(F.col("sqft") > 7603).select("city"))
    .orderBy("city")
)
