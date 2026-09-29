from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Bangkok", "Chiang Mai"])
    .agg(F.sum("rating"))
)
