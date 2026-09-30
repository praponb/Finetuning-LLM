from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date")

result = (
    df_store_sales
    .filter(F.col("units") < 18)
    .select("id", "sale_date", F.percent_rank().over(w).alias("sale_date_pct_rank"))
)
