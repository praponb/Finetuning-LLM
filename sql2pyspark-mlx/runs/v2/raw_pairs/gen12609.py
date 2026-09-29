from pyspark.sql import functions as F

result = df_shipments.select("id", "order_id", "carrier", "shipped_date")
