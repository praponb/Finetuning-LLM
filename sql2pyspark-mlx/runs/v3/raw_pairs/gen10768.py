from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Singapore", "London", "Berlin", "Bangkok"])
    .agg(F.sum("rating"))
)
