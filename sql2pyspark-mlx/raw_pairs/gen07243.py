from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("segment", ["SMB", "Consumer"])
    .agg(F.count("age"))
    .orderBy("country")
)
