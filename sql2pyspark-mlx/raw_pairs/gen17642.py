from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["UPS", "Kerry"]))
    .withColumn("delivered_date_month_end", F.last_day("delivered_date"))
    .withColumn("delivered_date_month", F.month("delivered_date"))
)
