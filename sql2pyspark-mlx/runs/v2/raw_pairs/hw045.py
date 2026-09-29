from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)

result = df_store_sales.select("id", "sale_date", "revenue", F.avg("revenue").over(w).alias("moving_avg_7"))
