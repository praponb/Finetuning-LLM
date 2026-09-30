from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["JP", "TH", "SG"]))
    .groupBy("country")
    .pivot("city", ["Singapore", "Chiang Mai"])
    .agg(F.avg("rating"))
)
