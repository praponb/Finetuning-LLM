from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") < 80) | F.col("shipped_date").between("2023-05-01", "2024-08-05"))
)
