from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Tokyo", "Singapore", "Bangkok"])
    .agg(F.max("rating"))
)
