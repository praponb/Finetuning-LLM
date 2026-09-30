from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc())

result = (
    df_store_sales
    .select("id", "store_id", "sale_date", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
    .orderBy(F.col("id").desc(), F.col("store_id").desc())
)
