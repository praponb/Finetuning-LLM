from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "SMB")
    .select("city")
    .unionByName(df_stores.select("city"))
    .distinct()
)
