from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") < "2024-09-01")
    .orderBy(F.col("id").desc(), "cost")
)
