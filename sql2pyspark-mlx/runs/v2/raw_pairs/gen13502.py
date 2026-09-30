from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Tokyo", "Bangkok", "Singapore"])
    .agg(F.max("rating"))
    .orderBy("country")
)
