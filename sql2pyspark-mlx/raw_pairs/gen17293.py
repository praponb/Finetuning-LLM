from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.year("shipped_date").alias("shipped_year"))
    .pivot("status", ["RETURNED", "LOST"])
    .agg(F.sum("cost"))
    .orderBy("shipped_year")
)
