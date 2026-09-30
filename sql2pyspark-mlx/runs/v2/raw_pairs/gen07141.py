from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") < 48)
    .select("carrier")
    .distinct()
    .orderBy("carrier")
)
