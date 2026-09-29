from pyspark.sql import functions as F

result = df_stores.select("id", "region", "opened_date")
