from pyspark.sql import functions as F

result = (
    df_departments
    .select("location")
    .intersect(df_stores.filter(F.col("sqft") > 5318).select("city"))
)
