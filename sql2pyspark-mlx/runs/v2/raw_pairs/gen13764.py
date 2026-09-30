from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("segment", ["Corporate", "SMB", "Consumer"])
    .agg(F.count("age"))
)
