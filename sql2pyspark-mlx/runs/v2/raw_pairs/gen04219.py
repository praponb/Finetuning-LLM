from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Bangkok", "London", "Singapore"])
    .agg(F.count("rating"))
)
