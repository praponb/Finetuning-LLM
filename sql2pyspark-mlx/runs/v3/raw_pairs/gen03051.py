from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("launch_date").desc(), "id")

result = (
    df_products
    .filter(F.col("stock_qty") != 148)
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "brand", "launch_date", "cost")
    .orderBy(F.col("id").desc())
)
