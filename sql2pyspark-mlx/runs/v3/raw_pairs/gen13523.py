from pyspark.sql import functions as F

result = df_shipments.filter(F.col("delivered_date").between("2025-09-01", "2025-10-01"))
