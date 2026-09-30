from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["JP", "TH", "SG"])
    .agg(F.sum("rating"))
)
