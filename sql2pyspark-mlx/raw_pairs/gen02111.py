from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .filter(F.col("quantity") < 7)
    .select("id", "quantity", F.percent_rank().over(w).alias("quantity_pct_rank"))
)
