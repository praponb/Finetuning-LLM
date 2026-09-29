from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("shipped_date_minus_25d", F.date_sub("shipped_date", 25))
    .withColumn("clean_carrier", F.trim("carrier"))
)
