from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("sale_date"), "store_id").orderBy(F.col("sale_date").desc())

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4, 5, 6]))
    .select("id", "sale_date", "store_id", F.rank().over(w).alias("sale_date_rank"))
    .orderBy(F.col("sale_date_rank").desc())
)
