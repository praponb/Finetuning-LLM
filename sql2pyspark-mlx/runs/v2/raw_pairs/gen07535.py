from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["DE", "US", "SG"])
    .agg(F.count("rating"))
    .orderBy("city")
)
