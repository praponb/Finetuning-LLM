from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .select("carrier")
    .distinct()
    .orderBy(F.col("carrier").desc())
)
