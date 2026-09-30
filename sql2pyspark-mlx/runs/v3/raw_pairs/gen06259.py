from pyspark.sql import functions as F

result = df_products.select("cost", "launch_date")
