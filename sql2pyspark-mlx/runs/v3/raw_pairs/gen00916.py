from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2023-03-01")
    .orderBy("id", "status")
)
