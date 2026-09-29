from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Singapore", "Tokyo", "Bangkok"])
    .agg(F.sum("rating"))
    .orderBy("country")
)
