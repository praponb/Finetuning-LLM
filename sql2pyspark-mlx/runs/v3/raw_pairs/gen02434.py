from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["DE", "US", "UK", "SG"])
    .agg(F.sum("rating"))
    .orderBy("city")
)
