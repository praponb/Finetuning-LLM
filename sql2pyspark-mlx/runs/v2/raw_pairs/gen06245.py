from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date")

result = (
    df_products
    .filter(F.month("launch_date").isin([4, 10, 12]))
    .select("id", "supplier_id", "launch_date", F.rank().over(w).alias("rnk"))
)
