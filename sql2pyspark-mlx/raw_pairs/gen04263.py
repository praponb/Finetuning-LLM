from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy(F.col("cost").desc())

result = (
    df_products
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "price", "cost", "launch_date", "tags", "supplier_id")
)
