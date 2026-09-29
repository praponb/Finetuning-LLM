from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["New York", "Bangkok"])
    .agg(F.sum("rating"))
    .orderBy("country")
)
