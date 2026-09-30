from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("segment", ["SMB", "Consumer", "Corporate"])
    .agg(F.count("age"))
)
