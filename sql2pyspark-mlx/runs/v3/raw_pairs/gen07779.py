from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc())

result = df_shipments.select("id", "cost", F.percent_rank().over(w).alias("cost_pct_rank"))
