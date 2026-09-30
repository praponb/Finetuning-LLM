from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc())

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 7]))
    .select("id", "store_id", "units", F.percent_rank().over(w).alias("units_pct_rank"))
)
