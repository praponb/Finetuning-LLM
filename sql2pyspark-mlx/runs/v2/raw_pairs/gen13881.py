from pyspark.sql import functions as F

result = df_products.filter((F.col("price") <= 1190) & (F.col("category") == "Home"))
