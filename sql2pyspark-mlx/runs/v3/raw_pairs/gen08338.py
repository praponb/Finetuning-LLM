from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") >= 71)
    .select("carrier")
    .unionByName(df_shipments.filter(F.col("carrier") == "Flash").select("carrier"))
)
