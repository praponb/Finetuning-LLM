from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("revenue").desc(), "id")

result = df_store_sales.select("id", "revenue", F.row_number().over(w).alias("revenue_row"))
