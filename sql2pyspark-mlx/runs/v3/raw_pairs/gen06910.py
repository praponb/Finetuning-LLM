from pyspark.sql import functions as F

result = df_shipments.select("order_id", "shipped_date", "status")
