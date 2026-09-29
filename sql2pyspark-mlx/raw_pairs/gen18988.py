from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .intersect(df_stores.filter(F.col("sqft").isNotNull()).select("city"))
    .orderBy("city")
)
