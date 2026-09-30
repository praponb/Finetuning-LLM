from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2023-07-04")
    .select("id", "cost", "carrier")
)
