from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status", "carrier")
    .distinct()
    .orderBy("status", "carrier")
)
