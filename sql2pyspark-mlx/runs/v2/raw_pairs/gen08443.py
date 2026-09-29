from pyspark.sql import functions as F

result = df_stores.filter((F.col("sqft") <= 9980) & ~F.col("store_name").like("M%"))
