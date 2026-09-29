from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "JP")
    .groupBy("country")
    .pivot("city", ["Bangkok", "London", "Singapore"])
    .agg(F.sum("rating"))
    .orderBy("country")
)
