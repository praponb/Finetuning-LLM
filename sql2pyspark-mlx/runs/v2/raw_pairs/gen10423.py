from pyspark.sql import functions as F

src = df_shipments.filter(F.col("delivered_date") <= "2023-07-01")

status_summary = (
    src
    .groupBy("status")
    .agg(F.max("cost").alias("max_cost"))
)

result = status_summary
