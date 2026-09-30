from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2024-06-05")
    .select("carrier")
    .unionByName(df_shipments.filter(F.col("carrier") == "DHL").select("carrier"))
    .distinct()
    .orderBy(F.col("carrier").desc())
)
