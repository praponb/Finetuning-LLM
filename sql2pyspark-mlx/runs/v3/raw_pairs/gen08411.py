from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["UK", "US", "TH"]))
    .groupBy("city")
    .pivot("country", ["SG", "JP", "UK"])
    .agg(F.max("rating"))
)
