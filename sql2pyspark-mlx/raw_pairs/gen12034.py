from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("status_upper", F.upper("status"))
    .orderBy(F.col("delivered_date").desc(), "id")
    .limit(20)
)
