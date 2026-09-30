from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Bangkok", "London", "Tokyo"])
    .agg(F.count("rating"))
)
