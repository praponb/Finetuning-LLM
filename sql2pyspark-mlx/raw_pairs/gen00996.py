from pyspark.sql import functions as F

result = df_stores.filter(F.month("opened_date").isin([2, 7, 9]))
