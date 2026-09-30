from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc())

valid = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-06-09").cast("date")) & F.col("product_id").isin([3, 4, 6]))
    .withColumn("revenue_whole", F.floor("revenue"))
)

result = (
    valid
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)
