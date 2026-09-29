from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Berlin", "London", "Tokyo"])
    .agg(F.avg("rating"))
)
