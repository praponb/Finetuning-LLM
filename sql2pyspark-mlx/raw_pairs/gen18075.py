from pyspark.sql import functions as F

result = df_shipments.select("cost", "delivered_date", "shipped_date")
