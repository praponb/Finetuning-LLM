from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["TH", "US", "SG"])
    .agg(F.max("rating"))
)
