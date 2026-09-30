from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("segment", ["Consumer", "SMB"])
    .agg(F.count("credit_limit"))
    .orderBy("city")
)
