from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.year("shipped_date").alias("shipped_year"))
    .agg(F.min("cost").alias("min_cost"))
)
